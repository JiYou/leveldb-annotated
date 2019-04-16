// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  // 注意，这里取出来的长度
  // 由于LookupKey是由三个部分组成的
  // key_len | key_data | (sn|type)
  // 并且这里的key_len = len(key_data) + 8
  // 所以这里取出来的字符串在比较的时候，实际上是只用了
  // key_data + (sn|type)
  // 注意type是放在后面的8个byte的开头
  // memcmp的时候，实际上就跑到后面去了，也就是最后比较
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

// 这里非常重要，在理解的时候，一定要注意到，aptr也好，或者bptr也好
// 原始的结构都是|key_len|key_data|(sn|type)
// 那么由于skiplist里面在比较的时候，需要按照字典序来进行比较。
// 所以这里实际上是
int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

void MemTable::Add(SequenceNumber s,      // 注意，每个key/value都是具有唯的sn
                   ValueType type,        // 只有两种类型
                   const Slice& key,      // 用户传进来的key
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8; // 就是用户传进来的key + sn(序号+类型一共64bits)
  // 编码后的长度就是四部分的长度之和
  // 注意：size都被编码过了
  // Q: 比较的时候不会很烦么?
  // dbformat.cc: int InternalKeyComparator::Compare()
  // 注意这里面比较的过程
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);

  // 注意这里处理的过程与LookupKey的内存布局是一样的
  // key_size | key_data | (sn|type)
  // 只不过这里的key_size是包含了key_data + (sn|type)一起之后的长度。
  // 所以前面的代码中 internal_key_size = key_size + 8;
  // 所以这里一定要注意到 (SN|TYPE)是放到后面的
  char* p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  // 注意，这里在放置的时候，是把整个key/value做为一个内存块，
  // 都放到了skiplist里面。
  table_.Insert(buf);
}

// Get()函数的第一个参数LookupKey
// 里面只会有用户传进来的key + sn
// 如果没有指定snapshot，那么这个sn就是当前db的最大的sn
// 注意LookupKey分为三个部分,key_len key_data sn
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  // 注意这里转换成为memkey.
  // 也就是memtable里面会用到的key
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  // 在memtable的skiplist里面的key是char*类型
  // 这里的内存是|key_size|key_data|(sn|type)
  // 注意在比较的时候，key_size位于前部，虽然是等于len(key_data) + 8
  // 但是里面并不包含(sn|type)的信息
  // 所以如果是两个相同的key，哪怕是带着不同的sn写入
  // 其前部的key_len都是一样的。
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64  (sn|type)
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.

    // 这里提取出user_key
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);

    // 如果user_key相等
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);

      // 找到KEY对应的类型
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          // 如果是值类型，取出相应的value.
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  // 如果user_key不相等，那么返回false.
  return false;
}

}  // namespace leveldb
