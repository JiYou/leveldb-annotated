// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

// Cache基类的析构函数，什么也不做
Cache::~Cache() {
}

namespace {

// LRU cache的实现
// LRU cache implementation
// Cache中的条目包含一个`in_cache`的bool值，标记着这个cache系统是否还关联着这个entry.
// `in_cache`被标记为false，那么cache中就不会有这个entry了
// 可能的原因是：
// 1. 通过Erase调用了`deleter`
// 2. 通过Insert插入了一个重复的值
// 3. 或者cache正在被析构
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
// cache里面包含了两条链表
// 所有cache中的条目必定位于在两条链表中的一条
// 但是不会两条链表中都有
// 仍然被client使用的entry，但是被cache移除之后
// 必然就不会在这两条链表中了
// 这两条链表：
// 1. in-use: 正在被client引用到的item, 并没有什么特别的顺序
// 2. LRU: 包含不被客户端引用到的条目，按照LRU的次序
// 条件会通过Ref()/Unref()这两个方法在这两条链表中移来移去
// 在这两个方法中如果发现`增加`/`减少`它的外部引用
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// 一个条目是一个可变长度的堆申请的结构体
// 条目被保留在特殊的双向链表中
// 并且会按照访问先后顺序来排序
// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
// 注意：这只是一个指针，指向cache中真正存放entry的内存块。
// 只不过指针并不能包含：
// 1. 是否在cache中
// 2. 如何销毁entry内存块, 内存块可以存放各种类型，所以并不能说强制转换成XX类型
//    然后强制调用析构函数
// 3. 包含字符串的key信息
struct LRUHandle {
  // 这里使用void*
  // 可以同时用于TableCache和BlockCache
  // 所以当用于BlockCache的时候
  // value就是block*
  void* value;
  void (*deleter)(const Slice&, void* value);
  // next_hash
  // 这里是用了hash开地址法来解决冲突
  // 也就是说，经过hash取模之后
  // 同一个hash的值会被放到同一个链表中
  // 这个单向的链表就是由next_hash来完成
  LRUHandle* next_hash;

  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  // cache只用来存放SST文件中的data block index结构
  // 或者data block结构
  // 里面只会有key/value
  // 所以这里干脆把key也放这里了
  size_t key_length;
  bool in_cache;      // Whether entry is in the cache.
  uint32_t refs;      // References, including cache reference, if present.
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  // 可变长的类
  // 里面存放着key/value中的key
  // 传进来的key会被复制
  char key_data[1];   // Beginning of key

  Slice key() const {
    // next_ is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
// 这里说是自己实现的hash表，速度比默认的hash表快
// 代码看下来：这个HandleTable应该是只负责hash表自己的内存
// 至于要添加进来的LRUHandle什么的，内存块，生命周期都不由hash表来负责
// 也就是说，HandleTable只负责:
// 1. list_这个数组
// 2. 要放进来的LRUHandle里面的指针归HandleTable管
// 3. 其他LRUHandle自身的内存块不由HandleTable管理
class HandleTable {
 public:
  // 构造函数里面的初始化也就只会创建4个item的空间
  // 类似于int **list_ = (int**)(malloc(sizeof(int*)*4));
  // 只不过这里需要把int换成LRUHandle
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  // 这里在清空HashTable的时候，只会去删除list.
  // 问题: refs如何处理？
  // 答案：LRUHandle本身的内存块不由HandleTable管理
  // 不需要在这里减减么？
  ~HandleTable() { delete[] list_; }

  // 查找的时候，通过key/hash来查找
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  // 插入一个条目
  // 考虑以下几种Case
  // Case 1. bucket为空，也就是所在地址的那个链表还没有建立起来
  // Case 2. bucket不空，但是没有找到item
  // Case 3. bucket不空，但是找到了相同item.
  // 在看注释的时候，先只看Case 1，把其他的Case遮住不要看!
  // 然后再只看Case 2.
  // 然后再只看Case 3. 
  // 注意: 当old不为NULL时，表示hash表中已存在一个与要插入的元素的
  //      hash值完全相同的元素，这样只是替换元素，不会改变元素个数
  // 问题：内存的释放怎么办？!
  // 答案： LURHandle内存块本身不由HandleTable管理
  LRUHandle* Insert(LRUHandle* h) {
    // Case 1. 这个时候返回的是bucket的地址，也就是&list_[i];
    // Case 2. 返回的是链表里面最后一个&(tail->next_hash)
    // Case 3. 如果能够找到相等的key/hash, 假设链表a->b->nullptr
    //         b->hash b->key与h的相等
    //         那么这里拿到的是&(a->next_hash)
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    // Case 1. 取得list_[i]
    // Case 2. 取得node->next_hash的值
    // Case 3. old = a->next_hash, 也就是old指向b结点
    LRUHandle* old = *ptr;
    // Case 1. 这个时候old肯定为nullptr
    //         那么新加入的结点的next_hash_就设置为nullptr
    // Case 2. old的值也是nullptr. 相当于拿到了tail->next_hash
    //         那么这里使h->next_hash = nullptr.
    // Case 3. 此时就不为空了, h->next_hash = old->next_hash
    //         h->next = b->next_hash
    //         指向相等元素的下一个结点
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    // Case 1. list_[i] = h; 实际上就是修改了头指针
    // Case 2. 相当于是修改了tail->next_hash
    //         tail->next_hash = h;
    // Case 3. a->next_hash = h
    *ptr = h;
    // Case 1. 如果没有找到相应的元素，那么这里新插入了一个entry/slot
    //         elems_加加.
    // Case 2. 如果旧有的tail->next_hash值，注意新的tail->next_hash
    //         已经指向h了
    if (old == nullptr) {
      // 新增加了元素
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        // 如果元素已经增长到了length_
        // 那么重新hash
        Resize();
      }
    }
    // Case 1. 这里返回旧有的list_[i]的值，也就是nullptr.
    // Case 2. 这里返回旧的tail->next_hash
    return old;
  }

  // 删除一个条目
  // 考虑以下几种Case
  // Case 1. bucket为空，也就是所在地址的那个链表还没有建立起来
  // Case 2. bucket不空，但是没有找到item
  // Case 3. bucket不空，但是找到了相同item.
  // 通过key/bash来移除一个item
  // 其中1. 2得到的*ptr都是为空
  // 所以不会有真正的删除动作
  // 那么下面的代码只考虑3.
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    // 这里返回值只可能有两种情况
    // Case 3:
    //      a. &list[i];
    //      b.  a->b->nullptr; 如果b->hash && b->key相等
    //          那么这里返回&(a->next_hash);
    LRUHandle** ptr = FindPointer(key, hash);
    // Case 3:
    //      a. result = list[i]的值
    //      b. result = a->next_hash的值，也就是b
    LRUHandle* result = *ptr;
    // 只考虑Case 3.那么result肯定不为空
    if (result != nullptr) {
      // *ptr理解成为前面的next_hash指针
      // 前面的next_hash指针指向找到的结点的后一个元素
      // 也就是前面的next_hash = b->next_hash
      *ptr = result->next_hash;
      --elems_;
    }
    // 返回找到的元素。
    return result;
  }

 private:
  // 这个hash table是由一系列bucket组成
  // 每个bucket是由一个链表组成
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  // length_是bucket的数目
  uint32_t length_;
  // elems_是存放的item的数目
  uint32_t elems_;
  // 指针，指向bucket数组
  // 一个bucket[i]就是LRUHandle*
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    // 这里取模的时候，用的是length_ - 1
    // 实际上是会把最后一个bucket给浪费掉
    // ptr这个时候，指向bucket的地址
    // 如果把bucket看成一个链表
    // ptr就指向链表头的地址
    // ptr == &list_[i];
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    // 如果不为空
    // 那么一直往前走
    // 这里取的是next_hash的地址
    // 然后解地址得到下一个结点的地址
    // 那么如果这个链表里面没有这个元素
    // struct node { struct node *next_; }
    // 假设 node a; a.next_ = nullptr;
    // 假设找不到的情况: 
    // a. 正常情况下的链表遍历是返回一个nullptr
    // b. 或者通过a.next_来判空，防止返回最后一个nullptr.
    // c. 这里是通过一个struct node **ptr = &(a.next_);
    //    如果发现*ptr == nullptr; 那么就返回ptr
    //    所以不会返回空指针，而是返回node的next_所在地址
    //    这个地址是有效的
    // 比较绕!
    // 假设能够找到相同的key/hash
    // a. 那么这里直接就返回了相应的slot的地址!
    //    比如，假设list[i]相等，那么，这里返回的就是&list[i]
    // 比如a->b->nullptr
    // 正常情况下是：如果b->hash == hash && b->key == key
    // 那么直接返回b
    // 这里返回的是&(a->next_hash);
    while (*ptr != nullptr &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    // 需要新申请的内存的长度
    uint32_t new_length = 4;
    // 如果新的长度小于elems_
    // 那么长度直接2倍
    while (new_length < elems_) {
      new_length *= 2;
    }
    // 新开辟内存空间
    LRUHandle** new_list = new LRUHandle*[new_length];
    // 清空内存
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    // count记录旧有的item数目
    uint32_t count = 0;
    // 这里依次遍历每个旧有的item
    for (uint32_t i = 0; i < length_; i++) {
      // 取出每个item
      // 这里是重新建立了一把hash
      // 处理第i条hash链表上的节点
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        // 保存下一个hash节点
        LRUHandle* next = h->next_hash;
        // 当前节点的hash值
        uint32_t hash = h->hash;
        // 与前面链表的方法类似，得到&new_list_[i];
        // re-hash，只不过这个i会发生变化。
        // 注意这里取模的技巧
        // 由于内存分配部是2^n
        // 所以 & (2^n-1)可以更加快速，而不是用%法。
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        // 相当于链表头插入法
        h->next_hash = *ptr;
        *ptr = h;

        // 移动到下一个结点
        h = next;
        // 计数加加
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// cache里面分了多个切片，每个切片负责一部分数据的LRU Cache
// 这里只是一个切片的具体实现。
// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  // 设置总共的capacity
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  // 内部函数
  // 注意这些函数都是被公有函数引用
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle*list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  // 总共的空间
  size_t capacity_;

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  // 已经使用的
  size_t usage_ GUARDED_BY(mutex_);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  // lru_链表的表头  dummy head
  // 这个链表中只会存放node.refs = 1并且in_cache==true的node
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  // in_use_链表的表头
  // 这个链表中只会存放node.refs >= 2并且in_cache == true的node
  LRUHandle in_use_ GUARDED_BY(mutex_);

  // 哈希表
  HandleTable table_ GUARDED_BY(mutex_);
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked lists.
  // 初始化链表
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  // in_use_的链表并不去处理
  // 问题：那in_use_里面的引用计数如何处理？
  // 答案：在要析构的时候，所有客户端对
  //      in_use_里面的节点的引用必须已经移除掉了
  //      也就是说，in_use_里面已经没有结点了
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  // 处理lru_链表
  // 这里面的节点是不消除的
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    // e表示当前要处理的结点
    // 记住下一个节点
    LRUHandle* next = e->next;
    assert(e->in_cache);
    // 设置in_cache为false
    e->in_cache = false;
    // 断言：因为lru_里面只存放只有一个的引用
    assert(e->refs == 1);  // Invariant of lru_ list.
    // 解引用
    // 这里refs会被减少到0
    // 减少到0之后，调用deleter函数，以及清除handle占用的内存
    Unref(e);
    e = next;
  }
}

// 增加对item的引用
void LRUCache::Ref(LRUHandle* e) {
  // 如果这个节点是在lru_链表里面
  // 那么需要从lru_链表移动到in_use_链表里面。
  if (e->refs == 1 && e->in_cache) {
    // 从LRU里面移出来
    LRU_Remove(e);
    // 然后append到in_use_链表里面
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

// Unref可以看成是降级
// 高级别: hash_table_ + in_use_ list
// 次级别: hash_table_ + lru_ list
// 低级别：递减为0，需要调用deleter并且释放内存空间
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
    // 这里确定已经不要在cache中了
    assert(!e->in_cache);
    // 释放handle指过去的block
    // 在cache中得到key/value
    // 如果是BlockCache，那么这里就是block*
    (*e->deleter)(e->key(), e->value);
    // 释放handle本身
    free(e);
  // 如果还在被引用，那么就从in_use_里面移出来
  // 然后放到lru_链表中
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

// 利用双向链表的删除功能，从双向链表中移出来
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

// 这里就是一个双向链表插入代码
// 这里是放到双向链表的表尾
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

// 在LRUCache中查找相应的条目
// 实际上是先在哈希表中查找item
// 然后增加对相应的item的引用
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  // 这里查找的时候，直接在hashTable中找
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    // 返回给客户端之后
    // 如果这个item是在lru_链表中
    // 那么需要把这个item移动到in_use_链表中。
    // 所以哈希表只负责找到相应的结点
    // 而结点在哪个链表中(lru_, in_use_)则是由LRUCache负责。
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

// 给客户端用的接口，用来解引用一个item.
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

// 客户端需要插入一个item
// 因为这个item会返回给客户端
// 当返回给客户端的时候，这个item的引用计数就变成了2
// 所以客户端一定要记住Release(返回handle)
Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);
  // 这里新生成一个LRUHandle
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  // 设置这个Handle
  e->value = value;
  e->deleter = deleter;
  // 如果是BlockCache，那么这里的charge是block.size();
  // 后面的capacity在处理的时候，只考虑value占用的空间
  // 而不算上key的size.
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  // 在这个Handle中需要加上key空间
  memcpy(e->key_data, key.data(), key.size());

  // capacity_ > 0表示的是打开了cache
  // 需要把这个item添加到LRUCache中
  if (capacity_ > 0) {
    // 这个时候，引用增加
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    // 这里是放到双向链表的表尾
    LRU_Append(&in_use_, e);
    usage_ += charge;
    // 这里调用FinishErase的原因是在于
    // 如果插入的元素在哈希表中也有
    // 也就是说有一个旧值，那么需要把旧值从表中移除掉。
    // 这样设计的原因是：
    // 这是一个KV系统，Cache是服务于读请求的
    // 单单对于读请求而言
    // 那么当相同的key被命中之后
    // 肯定是后来的key/value是有效的，
    // 所以这里采用的办法是直接把以前的
    // key/val移除出去
    // 当然，如果table_.Insert(e)返回值是空
    // 那么FinishErase()就什么也不做
    FinishErase(table_.Insert(e));
  // capacity_ == 0的时候，表示的是关掉了cache
  } else {
    // don't cache.
    // (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert,
    // so it must be initialized
    // 关掉cache
    // 这里应该类似于直接返回了
    e->next = nullptr;
  }
  // 如查使用空间大于capacity_
  // 并且lru_里面的空间不为空
  // 那么可能会从lru_中移除一些元素
  // 移除旧元素的时候是从表头开始删除
  while (usage_ > capacity_ && lru_.next != &lru_) {
    // lru_是一个带有dummy head表头的链表
    // 那么old就是指向链表中的第一个元素
    // 在while的判断中就设定了lru_中是必然存在元素的
    // 所以这里old必然是cache block而不是
    // 表头
    LRUHandle* old = lru_.next;
    // 在lru_链表中的item必然引用计数为1
    assert(old->refs == 1);
    // 这里是随机从hash table中找一个元素
    // 然后从table中移除出去
    // table中移除的时候，只是修改了next_hash指针
    // 并没有做其他的操作
    // Unref()则是在FinishErase中引用的
    // 至于解引用之后的生命周期，则是由最后解引用的那个人来负责
    // 释放内存块
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// 注意：这个函数的调用条件是
// e一定已经不在hash table中了
// 会从两个链表中扣出来.
// 从两个链表中扣出来之后，
// 实际上这个内存扣就已经不在cache中了
// 然后把这个内存块放野了
// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool LRUCache::FinishErase(LRUHandle* e) {
  // 从cache中移除这个handle的引用
  if (e != nullptr) {
    assert(e->in_cache);
    // 这里只是从某个双向链表中扣出来
    LRU_Remove(e);
    // 扣出来之后，就不再在hash table中了
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

// 要移除相应结点
// 这里需要做两步:
// 1. 从hash table中移出
// 2. 从cache中移出去
//    从cache中移出去的时候，实际上也就是从两个链表中移出去
//    至于内存块的生命周期，就不用再管了
void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

// 这里是清空lru_链表，因为这里面的元素都是没有被客户端使用到的
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

// SharededLRUCache
// 这里是把LRUCache分了16个区
static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  // 这里分区实际上有点提前做个简单的hash，使得每个
  // shard里面的LRU Hash Table里面的数目是1/16
  LRUCache shard_[kNumShards];

  // 用来保护last_id_的锁
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    // Hash函数，通过slice得到哈希值
    // 0表示一个随机种子
    return Hash(s.data(), s.size(), 0);
  }

  // 直接位移得到ShardID
  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    // 往上取shared的整数倍
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    // 每一个shard设置同样大的capacity.
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  // 析构函数，这个类只有一个shard数组，没有什么好释放的
  virtual ~ShardedLRUCache() { }
  // insert
  // 1. 找到hash值
  // 2. 得到shard id
  // 3. 放到相应的id的lruCache中去
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  // 查找
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  // 释放: 客户端释放这个引用
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  // 移除
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    // 1. 从hash表中移除
    // 2. 从两个链表中移除
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  // 生成新的id
  // 这个id应该是给不同的客户端使用的
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  // 清理空间，把不被客户端使用到的cache销毁掉
  virtual void Prune() {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  // 总共消费的空间
  virtual size_t TotalCharge() const {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
