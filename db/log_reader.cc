// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>
#include "db/log_reader.h"

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

/*
 * 总结一下读者的行为
 * 1. 跳过initial_offset_的block
 * 2. 跳过initial_offset_的record
 * 3. 然后一个一个record读出来。如果是{kFirstType, kMiddleType, kLastType}
 *    就把record都读出来，然后组装到scratch里面
 *    如果是kFullType，那么就只把结果放到record参数中，而不放到scratch中
 * 
 * 读record的步骤
 * a. 先把数据读到backing_store_里面
 * b. 利用backing_store_构建buffer_
 *    取record时候从buffer_中取
 *    end_of_buffer_offset_ - buffer_.size() - kHeaderSize - x
 *    是拿到刚读取的，长度为x的record的物理起始位置
 * c. {kFirstType, kMiddleType, kLastType}/kFullType需要拼接好了才返回
 *    给scratch.
 */
namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      // 一个block的memory buffer.
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {
}

Reader::~Reader() {
  delete[] backing_store_;
}

// Q 1. 如果initial_offset_为0，但是file给的时候，current位置不在
//      边界上应该怎么办？
//   应该是要求必须要求current pos在32KB上已经是对齐的。否则后面根据
//      initial_offset_来跳跃就没有意义。
// Q 2. initial_offset_不为0时是怎么处理的？
//
// 场景1： SkipToInitialBlock()中
//   const int leftover = kBlockSize - offset_in_block;
//   如果leftover == 6会发生什么？

bool Reader::SkipToInitialBlock() {
  // 
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  // 如果发现文件指针落在这个7bytes的尾巴上，那么直接跳过这个block
  // -6, -5, -4, -3, -2, -1
  // 这里代码写成：
  const int leftover = kBlockSize - offset_in_block;
  // 这里不能用<=
  if (leftover < kHeaderSize) {
    block_start_location += kBlockSize;
  }
  // 更加清晰明了
  // 实际上，这里如果是<=应该也都是可以跳过的?
  // leftover == 7是否需要跳过?
  // 1. 假设取== kHeaderSize的时候被跳过了
  //    那么万一前面一个block在写的时候，刚到余下7个btyes
  //    这个时候会放一个kFirstType 7byte，但是没有用户数据
  //    下一个block开始放kMiddleType
  //    如果跳过了，那么在读下一个block的时候，开始就读到
  //    kMiddleType, 接着会把整个用户数据跳过。
  // 如果按照原来作者这里的思路，当leftover == 6的时候
  // 也不跳，那么意味着会把相应的这个读出来。

  //if (offset_in_block > kBlockSize - 6) {
  //  block_start_location += kBlockSize;
  //}


  // (场景1): 当发生leftover == 6时，这个时候
  // block_start_location不会往前移动。那么后面在读的时候，仍然会从这个
  // 需要被跳过的block开始读。
  // offset_in_block则指向这个block的tailer部分。

  // 假设有一段内存区域0是与整个文件的current_pos_是对齐的。
  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  // 一开始是0，当然要跳过了
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  // 用户传进来的内存区域
  scratch->clear();
  record->clear();
  // 是否在一个切片的record里面
  bool in_fragmented_record = false;
  // 逻辑record上的offset.
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  // 临时记录下kFullType或者kFirstType类型的record 的起始位置.
  uint64_t k_first_record_offset = 0;

  Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);
    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    // 这里拿到的实际上是刚读出来的record的起始位置.
    // buffer_.size()是一个block里面还没有读出来的部分
    // kHeaderSize指的是刚读出来的slice的header头的大小
    // fragment是读出来的数据部分的长度
    // a. end_of_buffer_offset 指向的是buffer_尾部物理上的偏移
    // 注意，这里记录的是一个record的开头位置
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();
    // 注意: 如果record_type == kBadRecord
    // 那么fragment.size() == 0
    // 实际上就是physical_record_offset自动跳过了这个bad record.

    // fragment.size() == 0
    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        scratch->clear();
        *record = fragment;
        // 所以last_record_offset_指向的就是一个record的开头位置
        // 也就是刚读出来的record的开头位置
        last_record_offset_ = physical_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        k_first_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = k_first_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        // 注意kBadRecord会继续读
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    // buffer_虽然是由backing_store_构成，但是并不意味着
    // buffer_的大小一直是kBlockSize
    // 当从缓冲区中取走一部分数据之后，slice/buffer_内部的指针就
    // 会前移，然后buffer_的size就会变化了
    // (场景1): 这里会读取一个block出来
    //         一开始buffer_.size() == 0
    if (buffer_.size() < kHeaderSize) {
      // 如果还没有遇到尾吧，但是余下的空间是少于kHeaderSize的
      // 那么这里就需要重新取出block size.
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        // Read会把读取的数据存放到backing_store_
        // 然后利用backing_store_构建出
        // buffer_ = Slice(backing_store_, n);
        // n是最终读出来的数据大小
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size();
        // 这里是读取文件发生错误!
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          eof_ = true;
        }
        // 读完一个block之后，继续
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);

    // 这里是说，一个record里面出来的
    // const int record_size = kHeaderSize + length
    // if (record_size > buffer_.size())
    // 肯定出错了
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    // 如果读出来的类型是kZeroType
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    // end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length
    // 得到的就是一个record的开头位置。
    // 如果这个record的起始位置小于initial_offset_
    // 那么直接跳过
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    // 一个正常的record被读出来
    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
