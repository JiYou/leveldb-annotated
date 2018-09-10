// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// 写需要外部提供同步机制，大部分时候可能是一个mutex.
// 读只需要保证skiplist不会销毁就可以了
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.
// 除此之外，读线程并不需要其他额外的锁或者同步机制。
// Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants: 不变的特性
//
// 分配成功的节点永远不会删除，只有在skiplist在删除的时候才会释放内存
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.
// 这里保证是说并不提供删除的接口
// This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// 一个节点的内容除了前后指针之外，其他内容都是不可变的，当内容一旦放到skiplist
// 里面之后。只会有insert修改这个节点
// 这里采用的是release/store语义来保证单个节点的正确性
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template<typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;    // Arena used for allocations of nodes

  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;   // Height of the entire list

  inline int GetMaxHeight() const {
      // Your code here.
      return static_cast<int>(
          reinterpret_cast<intptr_t>(
              max_height_.NoBarrier_Load();
          )
      );
  }

  // Read/written only by Insert().
  Random rnd_;

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const {
      // Your code here.
      return (compare_(a, b) == 0);
  }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>
struct SkipList<Key,Comparator>::Node {
  explicit Node(const Key& k) : key(k) { }

  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
      // Your code here.
      assert(n >= 0);
      return reinterpret_cast<Node*>(
          next_[n].Acquire_Load()
      );
  }
  void SetNext(int n, Node* x) {
      // Your code here.
      assert(n >= 0);
      return next_[n].Release_Store(x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
      // Your code here.
      return reinterpret_cast<Node*>(
          next_[n].NoBarrier_Load()
      );
  }

  void NoBarrier_SetNext(int n, Node* x) {
      // Your code here.
      next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  port::AtomicPointer next_[1];
};

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
    // Your code here.
    char *mem = arena_->AllocateAligned(
        sizeof(Node) + (height-1) * (port::AtomicPointer)
    );
    return new (mem) Node(key);
}

template<typename Key, class Comparator>
inline SkipList<Key,Comparator>::Iterator::Iterator(const SkipList* list) {
    // Your code here.
    list_ = list;
    node_ = NULL;
}

template<typename Key, class Comparator>
inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
    // Your code here.
    return (node_ != NULL);
}

template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
    // Your code here.
    assert(Valid());
    return node_->key;
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Next() {
    assert(Valid());
    // Your code here.
    // 注意这里需要用acquire/load指令
    node_ = node_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Prev() {
    // Your code here.
    assert(Valid());
    node_ = list->FindLessThan(node->key);
    if (node_ == list->head_) {
        node_ = NULL;
    }
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
    // Your code here.
    node_ = list->FindGreaterOrEqual(target, NULL);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToFirst() {
    // Your code here.
    node_ = list->head_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToLast() {
    // Your code here.
    node_ = list->FindLargest();
    if (node_ == list->head_) {
        node_ = NULL;
    }
}

template<typename Key, class Comparator>
int SkipList<Key,Comparator>::RandomHeight() {
    // Your code here.
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < kMaxHeight && (rnd_.Next() % kBranching) == 0) {
        height ++;
    }
    assert(height > 0);
    assert(height <= kMaxHeight);
    return height;
}

template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
    // Your code here.
    return (n != NULL) && (compare_(key, n->key) > 0);
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev) const {
    // Your code here.
    Node *x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        Node *next = x->Next(level);
        if (KeyIsAfterNode(key, next)) {
            x = next;
        } else {
            if (prev) prev[level] = x;
            if (0 == level) {
                return next;
            } else {
                level--;
            }
        }
    }
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::FindLessThan(const Key& key) const {
    // Your code here.
    Node *x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        assert(x == head_ || compare_(x->key, key) < 0);
        Node *next = x->Next(level);
        if (!next || compare_(x->key, key) >= 0) {
            if (0 == level) {
                return x;
            } else {
                --level;
            }
        } else {
            x = next; 
        }
    }
}

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindLast()
    const {
    // Your code here.
}

template<typename Key, class Comparator>
SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(reinterpret_cast<void*>(1)),
      rnd_(0xdeadbeef) {
    // Your code here.
}

template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
    // Your code here.
}

template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::Contains(const Key& key) const {
    // Your code here.
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
