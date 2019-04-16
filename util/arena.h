// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "port/port.h"

namespace leveldb {

class Arena {
 public:
  Arena();
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  // 跟malloc一样的效果
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  // 分配对齐的内存
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  // 统计所有由Arena生成的内存总数
  // 这里面可能包含一些内存碎片
  // 所以返回的是近似值
  // 其实没有必要把memory_usage_设置成AtomicPointer的
  // 直接设置成一般变量就可以了
  // 比如size_t memory_usage_;
  size_t MemoryUsage() const {
    return reinterpret_cast<uintptr_t>(memory_usage_.NoBarrier_Load());
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  port::AtomicPointer memory_usage_;

  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  // 说这么多，实际上就是不能出现申请的bytes数小于等于0的情况
  assert(bytes > 0);
  // 如果当前块余下的空间还够用
  if (bytes <= alloc_bytes_remaining_) {
    // 得到当前块的指针头
    char* result = alloc_ptr_;
    // 移动指针
    alloc_ptr_ += bytes;
    // 更新余下的bytes数
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  // 当余下的空间不够用的时候，这里就去申请一个新块
  // 如果要的bytes数目是大于1k，那么就申请bytes那么多。
  // 如果要的bytes数目小于1k，那么新申请的时候，就
  // 按照4k来申请，并且从里面扣.
  // Fallback的意思是说退化，也就是退化成直接用new了。
  // 如果内存只是比alloc_bytes_remaining_大一点点，那么就不用了。
  // 直接去拿一块新的4KB，然后打散掉
  // - 如果要的本来就是一块大的 >= 1KB，那么直接把这个大的挂在vector里面。然后就返回了。
  // - 如果要的小于1KB，那么就申请一个4KB，然后开始切碎了返回之。
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
