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

// Arena::AllocateFallback的意思说，当Arena里面余下的内存不够的时候
// 就从系统的内存里面再申请一些内存
// 相当于退化成了New
char* Arena::AllocateFallback(size_t bytes) {
  // 当要申请的内存数量大于4KB
  // 那么直接拿了然后返回回去
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // 如果是小块的内存，也就是小于1kb的。那么申请一个大块4KB，然后
  // 从这一大块中扣一小块返回回去
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
  // 注意，这里取地址的时候，由于只能向前走。
  // 假设地址是0x07。并且对齐的时候，要求是8bytes对齐。那么
  // 当前的地址只能是再向前走一个byte才可以对齐。
  // 所以这里的slop就是计算出需要向前走的长度。
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  // bytes + slop，
  // 就是把余下的这个slop算在新的申请者头上
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
  // 这里比较有意思的是，AllocateFallback()函数里面要的都是
  // 4KB，并且是直接使用new来操作的。那么可以认为拿到的内存本来就是
  // 已经对齐的。
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
