// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

// 两级的Iterator
// 不管你是几级的迭代器，由于是继承Iterator
// 那么该有的接口还是要有
class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    // 其实也就是Table::BlcokReader
    BlockFunction block_function,
    // Table*
    void* arg,
    const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  // 注意：真正有用的都是data_iter_
  // index_iter是用来遍历
  // data block index里面的item的
  // 里面存放的key并不是用户真正传进来的key
  virtual bool Valid() const {
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  // 注意status里面的各种判断是否为空
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  // 跳过空的data block
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  // 初始化data block
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  // 第一级iter，由data block index:iterator
  IteratorWrapper index_iter_;
  // 第二级iter,由data block: iterator
  // 为了快速访问，这里使用了cache
  // 即IteratorWrapper
  // 因为经常会访问data_iter_ key和valid
  IteratorWrapper data_iter_; // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  // data block的offset/size
  // 这里称之为句柄
  std::string data_block_handle_;
};

// 构造函数，一开始设置第一级Iterator
// 第二级Iterator设置为空
TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {
}

// 析构函数啥也不做
// 那么后面需要注意看一下data_iter_的内存的释放
TwoLevelIterator::~TwoLevelIterator() {
}

void TwoLevelIterator::Seek(const Slice& target) {
  // 移动到对应的data block index所在的iter
  // 并不一定是移动到这个target
  // 移到最近的那个位置
  index_iter_.Seek(target);
  // 利用offset/size初始化相应的block
  InitDataBlock();
  // 看一下是否为空
  // 如果不为空，那么移动data_iter
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  // 如果data_iter_为空，或者说无效了，那么移动到下一个key/value.
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}


void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  // 如果data_iter为空
  // 或者不为空，但是走到底了
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // 如果index_iter_无效了
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    // 移动到下一个data block index.
    index_iter_.Next();
    // 初始化data block:iterator
    InitDataBlock();
    // 注意：生成iterator之后，并不代表着iterator已经存在于第一个item那里了
    // 有点类似于
    // std::vector<int>::iterator iter;
    // 声明之后，并不代表位于vector的.begin()位置。
    // 为了使用，这里还需要手动移一下到first.
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

// 当data_iter_走到一个block的边缘的时候，就需要走到下一个block的开始位置.
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    // 如果有效，那么就前向移动
    index_iter_.Prev();
    // 利用index_iter
    // 生成相应的data_iter
    InitDataBlock();
    // 移动到对应data block的最后一个key/value处
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

// 设置data iter的指向
// 如果不为空，那么先保存其状态
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  // data_iter_是一个iterator wrapper
  // 所以iter()会取到内部真正的iter
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  // 设置iter
  // Set函数会考虑原有的iter
  // 如果原有iter有指向一个block
  // 那么这里会把这个相应的内存释放掉
  data_iter_.Set(data_iter);
}

// 其实InitDataBlock这个函数名取得并不是特别好
// 取名叫GenerateSecondLevelIteratorFromIndexIterator()
// 这样更好，当然，名字太长了~
// 无论如何，我们知道这个函数的功能就是从一级iterator来生成二级
// iterator就可以了。
void TwoLevelIterator::InitDataBlock() {
  // 如果index_iter无效，那么设置data iter为nullptr.
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  // 如果data block index:iterator是有效的
  } else {
    // 取出offset/size
    Slice handle = index_iter_.value();
    // 如果已经指向这个block了
    // 那么就不用再操作了
    if (data_iter_.iter() != nullptr && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      // 这里直接生成一个新的iter
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      // 赋值新的offset/size
      data_block_handle_.assign(handle.data(), handle.size());
      // 设置新的iter
      // 并且会自动把原有的iter指向的内存释放掉
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options) {
  // 就是生成一个二级迭代器
  // 第一级是 data block index: iterator
  // 第二级由 block_function生成data block: iterator
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
