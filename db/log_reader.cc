// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"
#include <iostream>

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

/*
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

Reader::Reporter::~Reporter() {}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum)
    : file_(file),
      reporter_(reporter),  // 报错器，不用管
      checksum_(checksum),  // 是否需要对record进行校验
      // 一个block的memory buffer. 32KB
      // 因为写入的时候是按照32KB来进行编码的
      backing_store_(new char[kBlockSize]),
      // buffer_是一个slice，在backing_store_这个Block大小的内存区间里面移动。
      buffer_(),
      // 是否读到了文件尾?
      eof_(false),
      // 最后一个record的偏移
      last_record_offset_(0),
      end_of_buffer_offset_(0) {}

Reader::~Reader() { delete[] backing_store_; }

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
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
    // 实际上就是physical_record_offset自动跳过了这个bad record

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

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= 0) {
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
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length < 0) {
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
