// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
// MergingIterator
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator,
    Iterator** children, int n)
      : comparator_(comparator),
        // 注意，这里是对children进行了深拷贝
        // 所以需要重新生成n个iterator空间
        // 但是需要注意的是:Iterator是不支持复制的
        // 所以这里是通过重新生成IteratorWrapper
        // 包裹原来的Iterator来使用的。
        children_(new IteratorWrapper[n]),
        n_(n),
        // 当前的指向为空
        current_(nullptr),
        // 方向
        direction_(kForward) {
    // 依次设置每个IteratorWrapper里面的Iterator
    // 的值
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != nullptr);
  }

  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  virtual void Seek(const Slice& target) {
    // 全部都移动到那个>= target的那里
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    // 然后找个最小的
    FindSmallest();
    // 注意方向改变了
    direction_ = kForward;
  }

  // 注意Next()操作总是在找比当前key()要大的那个Iterator
  virtual void Next() {
    // 在往后面移动的时候，肯定需要是有效的。
    // 如果前面的Iterator已经是无效的，那么必然是不能再
    // 往后移动了。
    assert(Valid());

    // 这里需要确保所有的children都已经移动到了小于key()的地方！
    // 如果移动方向是forward即前向移动，那么
    // 因为current_已经是最小的children了，所有后面的Iterator的key
    // 肯定比当前key()大。
    // 否则，也就是说，当移动方向不是kForward的时候，需要显式操作non-current_
    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    // 这里这么多废话的主要原因是：
    // next()是只能前向移动，也就是找到一个比key()大的key()
    // 那么，如果移动方向不是前向的，就需要seek(key())
    // 然后再移动到刚好比key()大的地方
    if (direction_ != kForward) {
      // 那么遍历每个`Iterator`
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        // 如果当前的iterator不是current_
        // current_不需要seek()
        // 所以这里只移动其他的iterator
        if (child != current_) {
          // 那么把这个iterator移动>= key()的地方
          child->Seek(key());
          // 如果刚好与current_相等
          // 那么移动到下一个key
          // 注意这里判断了valid
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      // 把移动方向设置为前向移动
      direction_ = kForward;
    }
    // 由于是Next()
    current_->Next();
    // 然后再找出最小的kye()
    FindSmallest();
  }

  // 注意：按照LevelDB的Iterator的设计
  // Iterator的移动，如果是反向移动
  // 那么移动方向也会发生变化。
  virtual void Prev() {
    assert(Valid());

    // 反向移动，那么需要确保每个iterator都是在key的后面
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      // 如果不是反向移动，那么就需要先seek到>=key的位置
      // 注意：seek如果发现有== key的位置
      // 那么会优先停到==key的位置
      // 当移动到key的位置之后
      // 然后再向前移动一下。
      // 与前面next()同理，这里并不需要去调用current_
      // 的seek(),因为current_只需要Prev()一下就可以了。
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          // 如果valid才会移动
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          // 如果已经不是valid的iterator
          // 那么直接移动到最后一个iterator
          // 这样做的目的是什么？
          // 如果k路链表某一路已经合并完了。
          // 那么这一路置空应该就可以了啊？!!
          } else {
            // 虽然说是放到了链表的最后，实际上后面在合并的时候，还是容易带进来
            // 比如某一个路链表就是两个元素[1, 100]
            // 其他链表元素的区间都是在[1, ....., 200]
            // 那么[1, 100]如果只有两个元素，很快就在100, 1, 100, 1之间迭代。
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      // 修改方向
      direction_ = kReverse;
    }

    // 把当前指针向前移动
    current_->Prev();
    // 找到个最大的
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  // 比较器
  const Comparator* comparator_;
  // IteratorWrapper与Iterator本质上是差不多的。
  // 只是对以下内容做了一个缓存。
  // - key
  // - valid
  // 为了加速存取
  IteratorWrapper* children_;
  // children的Iterator的长度
  int n_;
  // 当前的iterator
  IteratorWrapper* current_;

  // Which direction is the iterator moving?
  // 向哪个方向移动?
  // 这里定义两个移动的方向
  enum Direction {
    kForward,
    kReverse
  };
  // 当前这个MergingIterator的移动方向
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  // 一开始最小的设置为空
  IteratorWrapper* smallest = nullptr;
  // 注意三种情况：
  // 1. 所有的children_[i]都是nullptr
  // 2. 某些为空
  // 3. 都不为空
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      // 如果为空
      if (smallest == nullptr) {
        smallest = child;
      // 如果非空，那么就需要比较
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        // 然后选择出最小的那个
        smallest = child;
      }
    }
  }
  // 更新最小的那个选项
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  // 找到所有iterator中最大的那个key
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

}  // namespace leveldb
