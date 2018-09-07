// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

// 各个type都会计算crc，由于type是固定的
// 那么这里直接把这些固定的值先计算下来
// 每次生成Writer的时候，都会来这里调用
// 这里每次都来计算生成，可能是考虑到计算量不大吧
static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() {
}

// 这里需要新生成一个Record.
// slice是需要存放的内容
Status Writer::AddRecord(const Slice& slice) {
  // ptr指向需要存放的数据
  // 这里并不关心用户数据是什么
  const char* ptr = slice.data();
  // 余下需要存放的数据的长度
  // 由于还没有开始存放，所以一开始的长度就是整个内容的长度
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  // 是不是要生成kFirstType?
  // 这个bool变量就是用来判断是不是生成了第一个record
  bool begin = true;
  do {
    // 在开始存放数据的时候，首先要看一下是否需要填空
    // 因为每个block后面都需要处理7 byte.

    // 看一下余下的空间
    const int leftover = kBlockSize - block_offset_;

    // 由于block_offset_指的是在一个block内部的偏移，所以这个
    // block_offset_肯定是在一个block内部
    // 也就是肯定满足
    // 0 <= block_offset_ < kBlockSize
    // 那么leftover肯定是大于0
    assert(leftover >= 0);

    // Case 1. 如果余下的空间不足以放一个kZeroType
    //         那么就需要把这部分空间填满0
    //         0 <= leftover < kHeaderSize
    if (leftover < kHeaderSize) {
      // 这个时候，leftover的值范围是
      // [0, kHeaderSize)
      // Switch to a new block

      // Case 1.1 如果余下的空间0 < leftover < kHeaderSize
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        // 其实这个assert可以提到前面
        // 为什么要放到这里assert?
        assert(kHeaderSize == 7);
        // 这里只需填充leftover个0.
        // 0的16进制表示就是\x00 ascii规定，\x后面必须用16进制
        // 由于是byte，也就是8bit，所以0需要用\x00来表示
        // 这里的Slice内部只是一个指针，指向这个常量字符串
        // leftover说明从这个字符串中取多少长度来使用
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      // Case 1.2 leftover == 0
      //          当leftover的空间为0的时候，block_offset_自动移到下一个
      //          block的开头，也就是0偏移处
      block_offset_ = 0;
    }
    // 实际上，如果leftover >= kHeaderSize
    // 那么就不会走前面那个if (leftover < kHeaderSize)

    // Case 2. leftover == kHeaderSize
    // 如果leftover == kHeaderSize
    // 那么就只会写一个header在这里。这个header的Type就取决于
    // 是否在slice的位置。如果是在中间，就是kMiddleType
    // 但是没有任何内容
    // 但是这里并没有分开来处理，而是统一进行了处理
    // 也就是代码规一化了。

    // Case 3. leftover > kHeaderSize
    // 余下的空间还可以存放一些数据。

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 能够取的数据的长度
    // fragment_length可以为0
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // 判断应该填入的类型
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    // 提交这个record.
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    // 注意：这里fragment_length可能为空
    // 比如一个block刚好余下7byte空间的时候
    // 那么这个空间会用来存放
    // |kMiddleType(0 length of data) |    <--- block X结束
    // |kXXXType(xx length of data) | ....| <-- block X+1开始
    // | .................................|
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

// 注意，这里n可以为0
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  // 长度n可以取0哦。这里n表示的是需要从ptr数据区里面读取的数据的长度
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  // header的结构: checksum 4 byte
  //            : length = 2 byte
  //            : type = 1 byte;
  // 所以buf的结构是
  // buf[0~3]是checksum
  // buf[4] = 长度的低8bit
  // buf[5] = 长度的高8bit
  // 这里是照按小端序来存放的
  // buf[6] = type
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  // type_crc_[t]直接拿到了type的crc值
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  // 为了存储，这里做了一点调整
  crc = crc32c::Mask(crc);                 // Adjust for storage
  // 大小端的转换
  // 如果是大端，需要转换一下
  // 然后存放到buf[0~3]
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 把转换后的结果放到buf里面
  // buf这个名字应该改成header_buf
  Status s = dest_->Append(Slice(buf, kHeaderSize));

  // 接着开始把数据写下去
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n));
    // 如果写成功，立马刷写磁盘
    // 这样做会导致写一个record，刷写一次磁盘
    // 是否可以改成一个slice刷写一次磁盘?
    if (s.ok() && t == kLastType) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
