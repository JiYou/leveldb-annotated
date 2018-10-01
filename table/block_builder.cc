// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                        // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +   // Restart array
          sizeof(uint32_t));                      // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  // 取得last_key，感觉可以直接使用last_key的。
  Slice last_key_piece(last_key_);
  // 一定是没有完成
  assert(!finished_);
  // 计数器不能超过block_restart_interval
  // 当==的时候，需要将shared的部分清0
  assert(counter_ <= options_->block_restart_interval);
  // 要么buffer_为空，要么新加进来的key肯定比之前加进来的key要大
  // 这里也就保证了key的有序性
  assert(buffer_.empty() // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  // shared的部分初始化为0
  size_t shared = 0;
  // 如果小于block_restart_interval
  // 那么需要去比较一下key
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    // 通过shared_key长度计算的时候，先取得最小的长度
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    // 在这个最小的长度里面，找到最相似的部分
    // shared也就是前缀相同的部分
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // 如果 == block_restart_interval
    // 那么需要重置restarts_和计数器
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  // 这里取得非共享的部分
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  // 将结果编码到内存中， 注意看格式
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  // 再把key/val放进去
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  // 设置last_key
  // 这里并没有去赋值last_key_ = key
  // 这是因为last_key_会去引用key的内存，而这段内存是不可靠的。容易出问题。
  // 万一key析构了，那么就出错了
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  // 计数器加加
  counter_++;
}

}  // namespace leveldb
