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
   // 这个只是用来汇错误的，在阅读代码的时候可以跳过
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
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
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
  char* const backing_store_;
  // buffer实际上就是backing_store_的皮
  // buffer_内部的data_指针就是指向backing_store_的。
  Slice buffer_;

  // 是否遇到EOF?
  bool eof_;   // Last Read() indicated EOF by returning < kBlockSize

  // 最后一次record读取成功后的offset位置
  // 只有读完kLastType或者kFullType之后才会更新这个值
  // Offset of the last record returned by ReadRecord.
  uint64_t last_record_offset_;

  // buffer_里面的offset
  // Offset of the first location past the end of buffer_.
  uint64_t end_of_buffer_offset_;

  // Offset at which to start looking for the first record to return
  // 一开始传进来的inital_offset_;
  uint64_t const initial_offset_;

  // True if we are resynchronizing after a seek (initial_offset_ > 0). In
  // particular, a run of kMiddleType and kLastType records can be silently
  // skipped in this mode
  // 当给定initial_offset_ > 0的时候，这个时候是需要跳过一些block的。
  // recyncing_就是在读取block的时候，判断是否需要跳过这些block
  // 1. 初始值为resyncing = initial_offset_ > 0
  //    当第一次读record的时候，就跳把这些block跳过。
  //    然后设置recyncing为false
  // 2. 第二次读的时候，直接无视resyncing_.
  bool resyncing_;

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

  // Skips all blocks that are completely before "initial_offset_".
  //
  // Returns true on success. Handles reporting.
  bool SkipToInitialBlock();

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
