// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/iterator.h"

namespace leveldb {

Iterator::Iterator() {
  // 初始化双向链表
  // 很奇怪的一点是：既然都用c++了
  // 那为什么不使用std::list<>
  cleanup_head_.function = nullptr;
  cleanup_head_.next = nullptr;
}

Iterator::~Iterator() {
  // 如果双向链表不是空的
  // 那么依次清空这个双向链表
  if (!cleanup_head_.IsEmpty()) {
    cleanup_head_.Run();
    for (CleanupNode* node = cleanup_head_.next; node != nullptr; ) {
      // 调用相应的回调函数
      node->Run();
      CleanupNode* next_node = node->next;
      delete node;
      node = next_node;
    }
  }
}

void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  // 注册清理函数
  // 如果使用std::list<>
  // 那么也就是一个push_back()搞定的事
  // 这里相当于是自己维护了一个双向链表 
  assert(func != nullptr);
  CleanupNode* node;
  if (cleanup_head_.IsEmpty()) {
    node = &cleanup_head_;
  } else {
    node = new CleanupNode();
    node->next = cleanup_head_.next;
    cleanup_head_.next = node;
  }
  node->function = func;
  node->arg1 = arg1;
  node->arg2 = arg2;
}

namespace {

class EmptyIterator : public Iterator {
 public:
  EmptyIterator(const Status& s) : status_(s) { }
  ~EmptyIterator() override = default;

  bool Valid() const override { return false; }
  void Seek(const Slice& target) override { }
  void SeekToFirst() override { }
  void SeekToLast() override { }
  void Next() override { assert(false); }
  void Prev() override { assert(false); }
  Slice key() const override { assert(false); return Slice(); }
  Slice value() const override { assert(false); return Slice(); }
  Status status() const override { return status_; }

 private:
  Status status_;
};

}  // anonymous namespace

Iterator* NewEmptyIterator() {
  return new EmptyIterator(Status::OK());
}

// 这里并没有限制Status的范围
// 这里按理说应该需要设置是所有error status
// 的种类
Iterator* NewErrorIterator(const Status& status) {
  return new EmptyIterator(status);
}

}  // namespace leveldb
