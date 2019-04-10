// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter, // 如果是immu_，那么这里就是指向的skiplist.
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  // 在level db的iterator设计中
  // 刚new 出来的iterator是不能直接用的，需要指向某个位置
  // 所以这里一开始就跳到最开始的位置上去。
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  // iter 有效的才进行如下操作
  // 如果无效，就不需要进行compact
  // 但是之前生成的number岂不是浪费了？
  // NOTE: 如果是mem table
  // 那么iterator的定义位于:
  // memtable.cc
  // class MemTableIterator: public Iterator {
  if (iter->Valid()) {
    // 如果iter有效，那么这里生成一个新的文件
    // 注意这里的用法: WritableFile*是一个纯虚类
    // 通过env->NewWritableFile()
    // 来生成真正的子类
    // 所以需要传递 &file进去
    WritableFile* file;
    // fname已经由dbname和前面申请的序号决定
    s = env->NewWritableFile(fname, &file);
    // 如果创建文件失败
    if (!s.ok()) {
      return s;
    }

    // Table builder是用来生成sst文件的
    TableBuilder* builder = new TableBuilder(options, file);
    // 第一个key当然是最小的
    meta->smallest.DecodeFrom(iter->key());
    // 然后依次遍历每个key
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      // 设置最大的key
      meta->largest.DecodeFrom(key);
      // 把每个key/value加到builder中
      builder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    // 这里会把在内存中的sst文件刷写到
    // 磁盘上
    s = builder->Finish();
    // 如果状态是OK的，那么这里更新一下meta信息
    // 并且检查一下文件的大小
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    // 处理完成，builder占用的空间可以释放掉了
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    // 最后再检查一把
    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  // 对于imm_的iter来说，
  // memtable.cc: class MemTableIterator: public Iterator
  // virtual Status status() const { return Status::OK(); }
  // 最终调用到这里
  // 所以内存形式总是返回ok.
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    // 如果有出错，那么删除这个文件
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
