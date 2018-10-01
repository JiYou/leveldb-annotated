// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_MERGER_H_
#define STORAGE_LEVELDB_TABLE_MERGER_H_

namespace leveldb {

class Comparator;
class Iterator;

// 返回一个Iterator, 它会提供children[0, n-1]里面数据的聚合
// 获得child iterators的所有权，当result iterator被删除的时候，并且会删除他们，
// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
// 结果并不保证无重复的限制
// 这就是说，如果一个key在K个child iterators里面出现了。
// 那么就会出现K次
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
Iterator* NewMergingIterator(
    const Comparator* comparator, Iterator** children, int n);
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_MERGER_H_
