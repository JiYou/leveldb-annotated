// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <stdint.h>
#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

class LEVELDB_EXPORT Cache;

// LRUCache的工厂类接口。
// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);

class LEVELDB_EXPORT Cache {
 public:
  // 这是一个纯虚类
  // 按理说是不能生成对象的。
  // 去掉复制接口
  Cache() = default;
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  // 基类的析构函数，主要是为了完整地释放内存
  // 实际上就是一个空函数。 {}
  virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  // 这个handle类似于一个Windows里面句柄的设计
  // 后面在使用的时候，都是引用的是Handle*。即其指针
  // 主要原因是：抽象类Cache在使用的时候，并不需要了解
  // Handle的内部函数与内部实现
  // 各种具体的实现，都是强制转换成为相应的指针来进行操作
  // 比如LRUHandle*，再后再操作`LRUHandle* ->`里面的各种函数
  // 为什么不把Handle设计成一个纯虚拟，像Cache这种纯虚类一样规定好相应的接口呢？
  // 这里面最主要的原因是：
  //  - Handle并不清楚各种底层实现的时候，每个Cache里面的entry需要的操作并不相同。
  //    所以也就无法形成统一的接口。
  struct Handle { };

  // 插入一个条目 key->value到cahe中，并且告知总共需要占用的空间。
  // 以便从总共的空间中扣除这个大小。
  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  // 插入之后，会顺便返回一个相应的handle.
  // 与传统的iterator调用者不一样的是，调用者获取Handle之后，需要
  // 调用cache->Release(handle)方法来把这个handle进行回收
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  // 当key/value不再被使用之后，将会使用deleter来进行内存的回收
  // 这里有个示例：
  //  block.cc::BlockReader()
  //     s = ReadBlock(table->rep_->file, options, handle, &contents);
  //     if (s.ok()) {
  //        block = new Block(contents);
  //        if (contents.cachable && options.fill_cache) {
  //          cache_handle = block_cache->Insert(
  //              key, block, block->size(), &DeleteCachedBlock);
  //        }
  //      }
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // 查找：在cache中查找一个key
  // 如果没有找到，那么就返回nullptr
  // If the cache has no mapping for "key", returns nullptr.
  // 否则如果找到相应的handle
  // 与前面caller一样。在使用完毕之后，需要调用cache->Release(handle)
  // 来释放对内存的引用。
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;

  // 释放一个handle.
  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // 获取Value
  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // 这里是将这个key从cache中移出去
  // 但是相应的key/value，还会保留
  // 只有当reference为0的时候，才会真正释放
  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // NewId() 接口可以生成一个唯一的 id
  // 多线程环境下可以使用这个 id 
  // 与自己的键值拼接起来，防止不同线程之间互相覆写.
  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

  // 移除所有cache中没有被真正使用到的item.
  // 内存限制的app可以调用这个接口来减少内存的使用。
  // 缺省的Prune接口不做任何事。
  // 子类被鼓励覆盖这个接口。
  // 将来的leveldb版本可以考虑把这个接口变成一个纯虚的接口
  // Remove all cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Default implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // leveldb may change Prune() to a pure abstract method.
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // cache.
  // 返回总共的消费
  virtual size_t TotalCharge() const = 0;

 private:
  void LRU_Remove(Handle* e);
  void LRU_Append(Handle* e);
  void Unref(Handle* e);

  struct Rep;
  Rep* rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CACHE_H_
