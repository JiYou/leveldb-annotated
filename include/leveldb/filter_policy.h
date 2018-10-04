// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A database can be configured with a custom FilterPolicy object.
// This object is responsible for creating a small filter from a set
// of keys.  These filters are stored in leveldb and are consulted
// automatically by leveldb to decide whether or not to read some
// information from disk. In many cases, a filter can cut down the
// number of disk seeks form a handful to a single disk seek per
// DB::Get() call.
//
// Most people will want to use the builtin bloom filter support (see
// NewBloomFilterPolicy() below).

#ifndef STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_
#define STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_

#include <string>
#include "leveldb/export.h"

namespace leveldb {

class Slice;

class LEVELDB_EXPORT FilterPolicy {
 public:
  virtual ~FilterPolicy();

  // 返回filter的名字
  // 如果filter过滤的方法有所更改的时候，这个Name也必须更改
  // 旧有的，与此不兼容的filters可能会被带进来使用
  // Return the name of this policy.  Note that if the filter encoding
  // changes in an incompatible way, the name returned by this method
  // must be changed.  Otherwise, old incompatible filters may be
  // passed to methods of this type.
  virtual const char* Name() const = 0;

  // 使用一系列keys，并且可能存在重复
  // keys[0,n-1] contains a list of keys (potentially with duplicates)
  // 但是是排好序的，排序的时候，使用的是用户传进来的comparator.
  // that are ordered according to the user supplied comparator.
  // 追回一个filter，可以总结出keys[0,n-1]的规则，并且把这个规则保存到dst里面。
  // Append a filter that summarizes keys[0,n-1] to *dst.
  // 注意：不要去修改dst里面的内容，你只能做append操作。因为dst里面可能会有其他的
  // 或者之前生成的filter的数据。
  // Warning: do not change the initial contents of *dst.  Instead,
  // append the newly constructed filter to *dst.
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst)
      const = 0;

  // 这里的filter，就是前面CreateFilter的dst.
  // 针对于`CreateFilter`传进来的一系列key[], 所有的这个key元素都必须 
  // 返回true.
  // "filter" contains the data appended by a preceding call to
  // CreateFilter() on this class.  This method must return true if
  // the key was in the list of keys passed to CreateFilter().
  // 针对于不存在的key，是有可能返回true的。但是，要求是
  // - 如果返回 不存在，那么肯定就不存在
  // - 如果返回  存在，实际上还是有可能存在的
  // 只是希望返回不存的概率高一些，如果太低的话，就总是要到文件里面去查找。
  // This method may return true or false if the key was not on the
  // list, but it should aim to return false with a high probability.
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

// 接口：返回一个新生成的bloom过滤器
// bloom过滤器有点类似于bits_per_key的过滤方式。
// 这里简单介绍一下过滤器：
// 给一堆32位的数字{a, b, c, d}。如果要建立一个最简单的过滤器。
// filter = a | b | c | d;
// 那么当新来的一个数字{x}的时候。
// 我们只需要去判断这个新来的数字{x}是否在对应位出现过1.
//
// cnt = 0;
// for bit in {x}:
//     x.{bit} == 1 && filter{bit} == 1:
//         cnt++;
// return cnt > 0;
//
// 实际上，这种处理方式对于不存在，那么就肯定是不存在。但是，如果bloom回答存在的时候
// 这个数实际上是有可能不存在的。
// 比如{a = 1, b = 2, c = 4, d = 8};
// 当输入一个数15，会得到一个存在的回答，实际上15并不存在。
// 但是，对于0，回答不存在的时候， 
// Return a new filter policy that uses a bloom filter with approximately
// the specified number of bits per key.  A good value for bits_per_key
// is 10, which yields a filter with ~ 1% false positive rate.
// 当所有使用这个filter的数据都关闭之后，调用者就需要删除这个filter.
// Callers must delete the result after any database that is using the
// result has been closed.
// 注意：如果你设计的比较器跳过了keys的某些字段。
// 那么你是不能使用NewBloomFilterPolicy()这个filter的。
// 并且你需要提供你自己的FilterPolicy，忽略相关的keys的部分。
// 比如：
//    如果比较器忽略了空白符。那么再使用一个BloomFilter过滤器的时候，并且这个
//    过滤器如果不是忽略了这些空白符的话，就会出现问题。
// Note: if you are using a custom comparator that ignores some parts
// of the keys being compared, you must not use NewBloomFilterPolicy()
// and must provide your own FilterPolicy that also ignores the
// corresponding parts of the keys.  For example, if the comparator
// ignores trailing spaces, it would be incorrect to use a
// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
// trailing spaces in keys.
LEVELDB_EXPORT const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_
