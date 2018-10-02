// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <assert.h>

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena() : memory_usage_(0) {
  alloc_ptr_ = nullptr;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;
}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

char* Arena::AllocateAligned(size_t bytes) {
  // 如果sizeof(void*)比8还要大，也就是遇到更高位的机器了？
  // 如果更大，就用sizeof(void*)来对齐
  // 否则就是用8来进行对齐
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  // 确保是2的指数次方
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  // 取得当前地址未对齐的尾数
  // 比如，要对齐的要求是8bytes
  // 但是当前指针指向的是0x017这里。
  // 那么余下的current_mod就是1
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  // 如果当前地址是0x17, 要求对齐是8 bytes
  // 那么current_mod = 1, slop 就是7
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  // bytes + slop，
  // 就是把余下的这个slop = 7算在新的申请者头上
  // 返回的时候，直接向前移动slop个bytes
  // 就完成了对齐。
  size_t needed = bytes + slop;
  // 总结一下：AllocateAligned就是需要计算一下
  // 对齐地址

  char* result;
  // 这里的逻辑就与Allocate完全一样的了。
  // 除了会移动一下slop以外。
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  // 这里断言一下，返回地址result肯定是对齐的。
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
  return result;
}

}  // namespace leveldb
