// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <stdint.h>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

class Reader {
 public:
  // Interface for reporting errors.
  class Reporter {
   public:
    // 这个只是用来汇报错误的，在阅读代码的时候可以跳过
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // 通过给定的文件file对象，从这个file里面读出log records
  // 也就是WAL  records.
  // Create a reader that will return log records from "*file".
  // 当reader还在使用的时候, file对象必段仍旧被使用
  // "*file" must remain live while this Reader is in use.
  // 如果reporter是非空的，那么当一些数据发现损毁坏需要被drop的时候，
  // 将会用来发送通知。当然notified的生命周期也必须比Reader要长。
  // If "reporter" is non-null, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  // 如果checksum被设置，那么就需要验证checksum.
  // If "checksum" is true, verify checksums if available.
  // 这里需要注意的是：initial_offset只是Reader想让文件产生的偏移
  // 而文件的当前位置并不一定保证在起始0位置。
  // 也就是ftell(fp)并不一定得到0
  // 实际上Reader也并不关心是否在offset 0处。
  // 不用管什么initial_offset，在整个代码中，initial_offset都是0
  // 实际上这个参数是可以删除掉的
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.
  Reader(SequentialFile* file, Reporter* reporter, bool checksum);

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  // scratch调用都需要提供的一段内存，主要作用是为了实现动态的内存管理。
  // 在ReadRecord里面可以放心地利用scratch来申请内存。
  // 当调用者忘了释放内存的时候，也可以自动释放。
  bool ReadRecord(Slice* record, std::string* scratch);

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  uint64_t LastRecordOffset();

 private:
  SequentialFile* const file_;
  Reporter* const reporter_;
  bool const checksum_;

  // backing_store_一个Block大小的内存区域
  // 一个backing_store_就是一个block
  char* const backing_store_;
  // buffer实际上就是backing_store_的皮
  // buffer_内部的data_指针就是指向backing_store_的。
  // buffer_会随着record的读取而移动
  // 所以buffer_里面记录的是一个block里面未读完的部分
  // end_of_buffer_offset_与buffer_是有联系的，注意看
  // end_of_buffer_offset_的说明
  Slice buffer_;

  // 是否遇到EOF?
  bool eof_;  // Last Read() indicated EOF by returning < kBlockSize

  // 最后一次record读取成功后的offset位置
  // 只有读完kLastType或者kFullType之后才会更新这个值
  // 注意，指向的是物理上offset_
  // 并且是record的开头部分。
  // Offset of the last record returned by ReadRecord.
  // 准确的讲，这个不应该叫last_record
  // 因为一个很长的slice也有可能被切分成多个record，但是这里并不记录
  // 中间那些不完整的record.比如kMiddleType的offset.
  //
  // 这里准确的意义应该说的是用户传给log_writer写入的时候，用户data
  // 的开头部分。
  // 因为后面的代码，只在kFullType和kFirstType的时候才会记录这个值
  uint64_t last_record_offset_;

  // 前面不是有个buffer_变量么？buffer_的尾巴与backing_store_的尾巴实际上是对齐的。
  // 因为操作流程如下：
  // 1. 从文件中读一个32KB出来到backing_store_中。
  // 2. 然后利用出来的内存空间backing_store_构建buffer_这个slice
  // 3. slice从头开始不断地吐出record
  // 4. buffer's end 's offset_ 指的就是backing_store_的尾巴的offset
  //    也就是下一次要读backing_store_的开始处。可以想象，应该是32KB对齐的。
  // Offset of the first location past the end of buffer_.
  uint64_t end_of_buffer_offset_;

  // Extend record types with the following special values
  enum {
    kEof = kMaxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
    kBadRecord = kMaxRecordType + 2
  };

  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(Slice* result);

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  // No copying allowed
  Reader(const Reader&);
  void operator=(const Reader&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
