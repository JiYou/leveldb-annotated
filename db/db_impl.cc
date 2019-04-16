// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

// 不需要建立table cache的文件数
const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
// 这里就是给写的时候用的
struct DBImpl::Writer {
  Status status;      // 写的状态
  WriteBatch* batch;  // 批量写入的batch
  bool sync;          // 是否需要刷写文件
  bool done;          // 写入是否完成
  port::CondVar cv;   // 这里用到的mutex与DB的mutex是同一个

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  // 一个反向指针，指向最开始的compaction信息块
  Compaction* const compaction;

  // 这里是说，由于每个key/value在写入的时候，都带了序号的。
  // 那么如果在compaction的时候，发现有duplicated key,
  // 并且这些keys'的sn里面有小于smallest_snapshot序号的
  // 那么这些老旧的key/value就可以被删除掉了。
  // 注意！在compaction的时候，一定有重复的key的时候，才会检这一项
  // 如果compaction的时候，发现key/value根本没有重复，那么这个key也是不会被删除的
  // 具体操作可以看一下DoCompactionWork这个函数。
  // 这里英文稍微有点歧义.
  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    // 文件的序号
    uint64_t number;
    // 生成的文件的大小
    uint64_t file_size;
    // 文件的起始key和终止key
    InternalKey smallest, largest;
  };
  // 有可能会有多个文件生成
  std::vector<Output> outputs;

  // State kept for output being generated
  // 输出文件句柄
  WritableFile* outfile;
  // sst文件生成器，负责将数据写入到磁盘上
  TableBuilder* builder;

  // 合并之后，总的生成的文件数
  uint64_t total_bytes;

  // vector的最后一个元素
  Output* current_output() { return &outputs[outputs.size()-1]; }

  // 显示的构造函数
  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
// 就是给定一个范围，当设置值小于最小值，就取最小值
// 当设置值大于最大值，就取最大值
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

/*
 * 修改option里面的各种设定：
 * 1. 修改参数范围
 * 2. 设置info_log
 * 3. 设置block_cache
 */
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  // 基于传进来的option
  Options result = src;
  // 设置key比较器
  result.comparator = icmp;
  // 过滤器, 先不管这个用来做什么
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  // 这里是把参数归一化，过大的数放到max_value
  // 太小的数放到min_value
  // 介于最大和最小之间的不做处理
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  // 中间操作记录，出错信息等等
  // 会把中间步骤与信息都写到这个文件里面
  // 这个文件放置的位置是dbname/info_log
  if (result.info_log == nullptr) {
    // 如果还没有日志指针，那么创建相应的目录
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    // 把原来的LOG文件重命名
    // ${dbname}/LOG -> ${dbname}/LOG.old
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    // 生成新的INFO LOG文件
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    // 如果不成功
    if (!s.ok()) {
      // No place suitable for logging
      // 那么依旧使用nullptr.
      result.info_log = nullptr;
    }
  }
  // 如果没有block_cache
  // block_cache就是用来存放sst文件里面的block数据部分
  // table_cache是用来存放sst文件里面的index cache部分
  // 这里并没有设置table_cache，后面可以看一下
  // table_cache是在哪里处理的。
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

// 这里设置table cache最大可以cache的文件
static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

// 这里raw_options里面可能会带进来一些参数，或者已经有的值
// 这里会有一些bool变量会查看一下当前是否会继续使用raw_option
// 中的值
DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    :
      // 初始化env环境变量
      env_(raw_options.env),
      // 初始化key比较器
      internal_comparator_(raw_options.comparator),
      // 初始化过滤策略
      internal_filter_policy_(raw_options.filter_policy),
      // 设置option:1. 参数. 2. info_log 3. block_cache
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      // 是否有info_log_
      owns_info_log_(options_.info_log != raw_options.info_log),
      // 是否有option中的 block cache.
      owns_cache_(options_.block_cache != raw_options.block_cache),
      // 数据库的名字
      dbname_(dbname),
      // table_cache指的是sst文件的index部分的cache
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      // 数据库的锁
      db_lock_(nullptr),
      // 是否关闭: Q: 这里为什么用指针？而不是用bool变量
      // 应该是考虑到原子性，如果这里是用cpp11，那么就应该用atomic<bool>
      shutting_down_(nullptr),
      // 后台线程信号:实际上就是指示compaction是否完成
      // background_work_finished_signal_.wait()就是等待后台的compaction完成。
      background_work_finished_signal_(&mutex_),
      // 活跃的mem
      mem_(nullptr),
      // 不能修改的mem
      imm_(nullptr),
      // WAL journal文件句柄，类似于FILE指针
      logfile_(nullptr),
      // WAL文件序号
      logfile_number_(0),
      // WAL文件写者
      log_(nullptr),
      // 随机数种子
      seed_(0),
      // 临时批量写
      tmp_batch_(new WriteBatch),
      // 后台合并调度
      background_compaction_scheduled_(false),
      // 手动合并
      manual_compaction_(nullptr),
      // 生成一个空的版本set对象
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {
  // 设置has_imm_为nullptr.
  has_imm_.Release_Store(nullptr);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-null value is ok
  // 在退出的时候，要等后台的compaction程序运行完成
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  // 清理锁文件
  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  // 这里都是在清理内存
  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  // 清理table_cache_
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}
/*
 * 总结一下NewDB()
 * 这个函数并不是直接给客户端使用的，而是被Open调用
 * 1. 初始化各种序列号
 *    0给WAL日志
 *    1给manifest文件
 *    2给将来要生成的新的文件编号
 * 2. 利用WAL日志文件将new_db这个版本编辑器dump到版本
 *    WAL日志manifest文件中
 * 3. 检查是否写入成功，如果写入成功，那么更新CURRENT文件
 *    如果失败，那么删除manifest文件
 */
Status DBImpl::NewDB() {
  // 生成一个空的版本编辑器
  // 注意，这玩意是一个version_edit
  // 并不是一个真正的db.
  // 当version_edit_没有基于任何version的时候，基本上可以看成是一个snapshot.
  VersionEdit new_db;
  // 设置DB name
  new_db.SetComparatorName(user_comparator()->Name());
  // 设置WAL编号: 也就是说WAL log把编号0占用了
  new_db.SetLogNumber(0);
  // 设置接下来的文件编号
  // 接下来新的文件会使用编号2
  // 那么中间的1呢？下面可以看到1是被Manifest文件用掉了
  new_db.SetNextFile(2);
  // 用户提交key/value时的编号
  new_db.SetLastSequence(0);
  // manifest文件的编号，这里新生成的DB里面把1占用掉了。
  const std::string manifest = DescriptorFileName(dbname_, 1/*这个是文件编号*/);

  // 接下来把这个生成新DB的操作通过写WAL日志的方式
  // 写到manifest文件中
  WritableFile* file;
  // manifest文件也是一个WAL文件
  // 这里是生成相应的文件句柄
  Status s = env_->NewWritableFile(manifest/*manifest_file_name*/, &file);
  if (!s.ok()) {
    return s;
  }
  // 把生成DB的操作写入到manifest文件中
  {
    log::Writer log(file);
    std::string record;
    // 这里是把db的各种信息编码到record里面
    // 然后写入到wal文件中
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  // 写入完成之后一，释放资源
  delete file;
  if (s.ok()) {
    // 如果写入成功，那么将CURRENT文件指向当前的新的manifest文件
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    // 如果失败，那么就取消这个记录
    env_->DeleteFile(manifest);
  }
  return s;
}

// 实际上只做了一件事，
// - 如果状态ok, 或者说不需要做检查
//   a. 啥也不做
// - 否则输出出错信息
//   b. 把状态设置为OK
// NOTE: 状态会被更改掉
// 
void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

// 这个函数要做的事
// live = pending_outputs_ + files_in(version_set_);
// all_files_in_dir = GetDBFiles()
// to_delete_files = all_files_in_dir - live
void DBImpl::DeleteObsoleteFiles() {
  mutex_.AssertHeld();

  // 如果后台发生了错误，并不清楚一个新的版本是否或者没有被提交，所以这里为了
  // 安全起见，并不会去做垃圾收集
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  // pending_outputs_里面肯定是活着的文件
  // 这个作为一个基础
  std::set<uint64_t> live = pending_outputs_;
  // 然后再把version_set_里面所有的文件都放到live
  // 中
  // 达到的效果是
  // live = pending_outputs_ + files_in(version_set_);
  // 这里会把version_set_里面的每个version以及每个version的LEVEL里面的
  // 文件都放到这里面来
  versions_->AddLiveFiles(&live);

  // 拿出所有db/目录里面的文件
  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  // 序号
  uint64_t number;
  // 类型
  FileType type;
  // 遍历所有的文件 
  for (size_t i = 0; i < filenames.size(); i++) {
    // 取出文件的序号，类型
    if (ParseFileName(filenames[i], &number, &type)) {
      // 默认是保留这个文件
      bool keep = true;
      switch (type) {
        // WAL日志文件只保留两个：
        // 注意wal文件的回收，会在两个时候发生，一个是
        // 1. 打开db的时候，这个时候会把wal里面的内容读出来，放到skiplist中
        // 然后再dump文件到level 0
        // 2. 当把imm_里面的内容dump到level 0的时候
        // 实际上，相当于把WAL LOG里面的内容整理了一把放到了level 0
        // 这个时候，只需要把wal log扔掉，重新生成一个就可以了。
        // current log number
        // pre log number
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        // MANIFEST文件的WAL日志文件
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          // 只保留当前的版本
          // Q: 不存snapshot么？
          // 这是因为每次生成manifest新文件的时候，都是需要写一次snapshot
          // 所以manifest文件的开头就是一个version_set_的snapshot
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          // 如果是sst文件，那么如果不在live里面，就要干掉
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // 临时文件必须被输出到pending_outputs_里面
          // 否则就删除
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        // 其他CURRENT/lock/LOG文件，都保留
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        // 如果不保留
        // 那么从TableCache中移除
        // 注意理清cache
        // 那么类似，是不是block_cache是不是也可以被清理一下？
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        // 真正删除文件
        Log(options_.info_log, "Delete type=%d #%lld\n",
            static_cast<int>(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  // 1. 创建目录
  env_->CreateDir(dbname_);
  // 2. 既然是要恢复，那么肯定是没有其他人拿到了锁
  assert(db_lock_ == nullptr);
  // 3. 创建文件锁
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  // 如果失败，那么说明其他地方有人拿了数据库的锁。
  // 可能是别的线程或者进程
  if (!s.ok()) {
    return s;
  }
  // 4. 如果current文件不存在
  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      // 通过CURRENT文件和create_if_missing
      // 这两个条件来决定是否需要生成数据库
      // NewDB并不是仅生成一个数据库内存对象
      // 主要功能还是生成一个VersionEdit
      // 然后将这个“生成新DB"的操作持久化到
      // manifest文件中
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    // 如果存在则报错
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }
  
  // 此时的versions_这个version_set_里面还啥都没有，就是一个空的双向链表
  // 这里会从manifest里面将各种VersionEdit读出来
  // 然后apply到一个version上。再把这个version放到version_set_双向链表上
  // 在这之前也会有一个current_ version
  // 但是在那个version里面，里面什么都没有
  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }

  // 最开始的SN设置为0
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  // 取出version_set里面的log_number
  // 取出之前的log number.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  // 扫描目录下的所有的文件
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }

  // live文件集合为空
  // 这里并不是说把expected里面的内容放到live里面去
  // 而是扫描整个version_set里面所有的version
  // 所有level文件，都会被加到expected里面。
  std::set<uint64_t> expected;
  // 因为已经读出了所有的manifest里面的record
  // 所以versions_里面已经记录了所有的files.
  versions_->AddLiveFiles(&expected);

  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  // filenames是一个db目录下的所有的文件
  for (size_t i = 0; i < filenames.size(); i++) {
    // 能够正确解析的文件形成的集合，总是会包含expected
    if (ParseFileName(filenames[i], &number, &type)) {
      // 最后expected必然要为空才行
      // 不能说version_set里面有很多文件，但是这些文件又不存在于磁盘上
      expected.erase(number);
      // 取出有效的log文件
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  // 磁盘上可以解析的文件
  // 肯定是expected的父集
  // 所以一阵处理之后，expected肯定为空
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // 将logs文件排序
  // 然后依次重放log文件
  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    // 这里就是依次把wal log里面的内容整理之后扔到level0
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    // 把log[i]的序号设置为已用。如果log[i]的序号太新的话
    versions_->MarkFileNumberUsed(logs[i]);
  }

  // 更新sn
  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

// 实际上就是把WAL LOG里面的内容整理一下放到level0
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  
  // 出错处理，不管
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  // 生成wal 文件的文件名
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  // 开始生成文件句柄
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  // 出错处理的reporter，不管，不是核心代码，直接跳过
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  // 生成一个wal log的读取器
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;

  // 依次读出wal log里面的key/value
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    // 如果record的格式有问题
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }

    // 生成一个写item
    WriteBatchInternal::SetContents(&batch, record);

    // 生成一个skiplist
    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    // 把写请求放到skiplist里面
    status = WriteBatchInternal::InsertInto(&batch, mem);

    // 这里只是看一下是否需要报错，并清整理status的状态。
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }

    // 更新sn，注意sn和count也有关系
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      // 如果需要生成level0的文件，那么就直接写到level0
      compactions++;
      *save_manifest = true;
      // 注意：这里会有放掉锁的风险。
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  // 最后都写入到level0里面
  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  // 记录一下时间，不用管
  const uint64_t start_micros = env_->NowMicros();

  FileMetaData meta;
  // 向db要一个新的file_number
  // 由于现在已经有锁了，所以直接++就可以了
  meta.number = versions_->NewFileNumber();

  // 这里记住这个文件的编号正在准备要写入到磁盘上，
  // 或者磁盘上的这个文件正在被写入
  // 并且这个文件还没有放到levels里面
  // 放到pending_outputs_里面，主要是为了防止误删除
  // 注意看DeleteAbsoluteFiles()这个函数
  pending_outputs_.insert(meta.number);

  // 因为后面要遍历整个immu_
  // 所以这里先拿一个iterator出来
  // 注意，刚拿出来的时候，这个iterator还是不能直接被使用的
  // LevelDB里面设计的Iterator生成的时候，都是与STL的不一样。
  // 需要先assign一下位置，才可以继续使用
  Iterator* iter = mem->NewIterator();
  // 打印消息，不用看
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    // 生成sst文件
    // 这里把锁释放掉
    mutex_.Unlock(); // 这里写磁盘的时候，把锁放掉
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    // 重新拿到锁
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  // 文件已经刷写到了磁盘上
  // 这里是否应该等加到某个level之后再从pending_outpus_中移出?
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  // 看一下放到哪个层级
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    // 如果base version不为空
    // 那么想办法找到最适合的level来进行输出
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    // 把file添加到某个级别
    // 当然默认是level 0
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  // 记录一下compact的状态
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros; // compact的用时
  stats.bytes_written = meta.file_size; // 写到磁盘上的size
  stats_[level].Add(stats); // 添加状态,也就是这个level上的compact状态
  return s;
}

// 由于CompactMemTable基本上都是后后台线程trigger的
// 而后台线程的在BackgroundWork()那里会去拿到锁。
// 所以这里肯定是已经拿到锁了
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  // version 常量类型是描述一个静态的状态的，不可更改。
  // 比如手里有一个苹果。能够清晰准确地传达手里面苹果的数量为1。
  Version* base = versions_->current();
  // 这里对current增加引用
  // 详细的可以看一下https://zhuanlan.zhihu.com/p/44584617
  // 主要是version里面记录了各个层次的文件。
  // 如果某些文件因为合并之后，不再被读取，那么这些文件
  // 就没有存在的必要了。是可以被删除掉的。
  base->Ref();

  // delta类型是一种操作，比如：来，给你一个苹果。
  // 这个delta类型是增加的，并不能清晰的描述出手里苹果的数量。
  // VersionEdit就是这种delta类型.
  // VersionSet::Builder则是完成加号的操作
  // A + B + B = D
  // 在levelDB中则需要写成如下形式
  // VersionSet::Builder build(A); // 这里把A做为基础项
  // build.Apply(B);               // 累加B
  // build.Apply(B);               // 累加B
  // build.SaveTo(&D);             // 把结果保存到D
  VersionEdit edit;
  // 这里把imm_写到文件中去
  Status s = WriteLevel0Table(imm_, &edit, base);

  // imm_写到新文件之后，把原来的base version解引用
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    // 不用管这个prev log number
    // 没有什么用的
    // 完全是为了兼容旧代码
    edit.SetPrevLogNumber(0);
    // Q: 这是一个version_edit_，这里会引用到
    //    logfile_number_
    //    按理说version_set/version_edit_修改的应该主要是
    //    manifest文件做为wal_file
    //    所以这个文件是否是manifest_file_的序号?
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    // 把edit即对于version_set_的改动放到version_set中
    // 针对于version_set_的改动主要是集中于level[].compact_key
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

// MaybeScheduleCompaction()是所有的compaction的入口
void DBImpl::MaybeScheduleCompaction() {
  // 确保mutex已经在手里了
  mutex_.AssertHeld();
  // 这里面遇到以下情况是不需要做compaction的.
  // 如果后台已经在compaction了
  if (background_compaction_scheduled_) {
    // Already scheduled
  // 如果需要关闭数据库
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  // 如果后台出现了错误
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  // 或者说并不需要compaction
  // 那么这里立马返回
  } else if (imm_ == nullptr &&
             manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
  // 后台的compaction开始调度
    background_compaction_scheduled_ = true;
    // 生成一个任务，放到任务队列中
    // 然后开始处理
    // 这里实际上就是去调用BackgroundCompaction()
    // NOTE: 这里是一个异步操作，只是把任务扔到Queue
    // 里面开始返回。
    // MaybeScheduleCompaction相当于
    // TriggerCompactionAction
    // 简单地触发一下之后，然后就开始返回
    // 由于是异步，那么返回之后，db的锁很有可能已经释放掉了
    // 所以，真正的BGWork即->BackgroundCall()
    // 开始的时候，需要重新去拿锁。
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

// 这里是后台任务在执行
// 如果同时存在写入线程，与后台任务线程在跑，那么这里就会
// 同时去拿锁。
// 需要注意的是：MaybeScheduleCompaction()是需要在持有锁的情况下执行的。
// 所以mutex_.AssertHeld();
// 相当于代码结构是
// Thread 1.
// DBImpl::Write() {
//     MutexLock l(&mutex_);
//     // in some case need to trigger compaction.
//     MaybeScheduleCompaction(); <-- 这里实际上并不真正执行compaction.
//                                    // 只是生成一个task结构放到后台Queue中
//     // 完成之后，mutex被释放
// }
// 后台线程中的Task item被解开之后，开始要来持行compaction.
// Thread 2.
// DBImpl::BackgroundCall() {// 后台任务
//     MutexLock l(&mutex_);
//     // 准备compaction.
// }

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  // 既然已经开始执行，那么肯定已经经过了调度
  assert(background_compaction_scheduled_);
  // 如果要关闭了
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  // 后台是不是出错了
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    // 如果啥事都没有，那么开始准备compaction吧。
    BackgroundCompaction();
  }
  // 注意，这里调度完成之后，需要把这个bool变量设置为false.
  background_compaction_scheduled_ = false;
  // 因为在接下来的MaybeScheduleCompaction()会再次设置
  // 这个变量，把这个变量设置为true.

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  // BackgroundCompaction()有可能会在某个层级产生太多的文件
  // 根据需要，这里就开始调度生成另外一个compaction来处理这些
  // 新生成的过多的文件。
  MaybeScheduleCompaction();
  // NOTE：这里重新trigger compaction。有点类假于
  // thread + queue + 异步的递归调用
  // 只不过这个是在线程情况下，使用queue + task_item来完成的。
  // 注意与通常的函数写法上的递归调用的不同。
  background_work_finished_signal_.SignalAll();
}
/*
 * 简要总结一下下面这个函数所做的事情
 * 1. 如果发需要做mem compaction. -> do -> return
 * 2. 检查是否需要manual compaction，更新相应区间:
 *    a. manual compaction的区间信息
 *    b. size/seek compaction的区间信息
 * 3. 是否可以trivial compaction
 * 4. compaction work
 * 5. 更新compaction状态
 */
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  // 最高优先级1
  //  memtable compaction.
  // 因为mm_是满足大小为4MB的时候才会转化成为imm_
  // 所以这里如果发现为非空，那么直接就
  // compact memtable.
  if (imm_ != nullptr) {
    CompactMemTable(); // 这里比较简单，就是把内存文件写入到level0
    // 至于生成了太多level0文件，需要compaction。那么就是在
    // BackgroundCall() -> MaybeScheduleCompaction()
    // 再次触发来完成的了。
    // 有点像形成了递归调用。
    // 只不过是基于后台线程 + Queue的递归调用
    // 并且是异步的
    return;
  }

  Compaction* c;
  // 看一下是否设置了手动compaction.
  // 在ceph中会主动调用，进而触发compaction.
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end; // 记录manual compaction的尾巴
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    // Compact指定level的指定区间
    c = versions_->CompactRange(m->level, m->begin, m->end);
    // 看一下是否结束
    m->done = (c == nullptr);
    // 如果compaction没有结束，那么是需要更新一下manual_end
    if (c != nullptr) {
      // 这里是通过compact result拿到最终的那个largest_key
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do

  // 这个if的判断是说，如果不是manual的，并且可以直接把文件并到高层去。
  // 那么就直接移动一下就可以了。
  } else if (!is_manual && c->IsTrivialMove()) {
    // 是否需要移动Trivial：不重要的，微不足道的
    // 返回 True,trivial Compaction，则直接将文件移入 level + 1 层即可
    // 也就是说，这个文件比较独立，可以直接移动到更高的层级
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }

    // 这个不用管，LevelSummaryStorage只是用来生成可读性较强的信息。
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  
  // 如果不能把level i的文件放到level i + 1层上去
  // 那么就老老实实干合并的工作吧。
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  // 如果c为空，这里会不会炸掉?
  delete c;

  // 这里只是简单地看一下是否需要输出compaction的错误信息
  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    // m没有别的用途，就是为了后面写变量名不用写manual_compaction_这么长
    ManualCompaction* m = manual_compaction_;
    // 如果状态不好，那么直接设置为OK
    if (!status.ok()) {
      m->done = true;
    }
    // 如果没有完成，那么需要更新trigger manual compaction里面的局部变量
    // 作为提示信息，返回给调用者，让调用者知道compactoin的状态
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end; // tmp_storage就是内部类一个非常临时的中转站
      // 可能是因为m->begin类型就是设置成为一个指针类型。
      // 这个时候，突然要指向一个区间段的中间的元素，这时
      // 只能是在类内部生成一个临时变量
      m->begin = &m->tmp_storage;
    }
    // 这里更新掉之后，表示不再引用?
    // 注意看一下，manual_compaction_只是一个指针
    // 由触发manual compaction的函数，在函数的内
    // DBImpl::CompactRange(b, e) {
    //    temp_m = ManualCompaction();
    //    db->manual_compaction_ = &temp_m;
    // }
    // 所以这里使用完成之后，表示manual compaction已经完成
    // 所以这里需要置空
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);

  // 如果还没有做过snapshot，那么这里直接拿到最大的SN
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    // 如果做过snapshot，那么这里直接拿到最小的snapshot sn
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock(); // <-- 由于要操作的是磁盘上的数据结构，所以这里并不需要持有db内存控制结构的锁

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != nullptr) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          // 这里是说拿到了一个新的key
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      // 到这里的时候，如果发现key == pre_key
      // 那么前面Compare() == 0
      // 并且last_sequence_for_key = pre_key.sn
      // 如果last_sequence_for_key <= compact->smallest_snapshot
      // 那么说明当前的key的sn必然也是小于compact->smallest_snapshot
      // 的。所以dro就成了定局
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) { }
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);  // 这里一开始就去拿到锁

  // 设置读的snapshot
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    // 如果传进来的参数里面没有snapshot
    // 那么就取最大的序号
    snapshot = versions_->LastSequence();
  }

  // 锁定需要的内存结构
  // 后面在释放的时候，不会被别的模块释放
  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  // 这里可以思考一下。如果在使用的时候，compaction也在使用imm_应该怎么办？
  // 真正在实现的过程中，由于compaction完成之后，会调用UnRef()函数
  // 谁是最后一个UnRef()的，那么就由谁来释放内存。
  // 所以这里不需要内存的争用问题
  // 但是，由于imm_ mem_里面的变量的引用计数都不是原子类型，都是需要在拿到锁的时候
  // 才可以进行的操作，所以这里需要在拿到锁的时候才可以Ref()/UnRef()
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  // NOTE: 在设计的时候，skiplist被设计成多读一写。也就是说支持多个线程同时读
  // 但是同一时刻只能有一个线程写，主要的原因是因为，skiplist不会删除原来的节点
  // 也不会修改原来的节点。
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    // LookupKey主要由三个部分
    // - start  key_len part
    // - kstart key_data part
    // - end sn/seq uint64_t

    // 在skiplist中查找到的时候，会用到start-end
    // 在sst中查找的时候，只会用到kstart - end
    // Q: 这里放进去的sn是当前全局最大的sn
    // 寻么在查找的时候，就需要看一下如何利用这个sn
    // 找到最新的key
    LookupKey lkey(key, snapshot);

    // 三段击
    // 先在mem中找
    // 再在imm_中找
    // 单纯从代码来说，mem_与imm_是同样的数据结构。
    // 最后在current version中找，实际上就是在磁盘中找了
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      // 这里在查的时候，是基于current version
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != nullptr
       ? static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number()
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

/* 简化地总结一下写入的流程：
 * - 放到Queue中
 * - MakeRoomForWrite()
 * - 打包所有的写入请求
 * - 写入到wal log里面
 * - 写入到MEM_里面
 */
// 如果是从put函数过来。那么在DBImpl::Put那边。
// my_batch那头就是一个局部变量
// 当Write调用完之后那边释放
// NOTE: 如果my_batch == nullptr
// 那么用意就是强制来一波compaction.或者等待后面的compaction完成
Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  // 这里并不会对mutex_上锁。只是用来生成writer里面的cond变量
  Writer w(&mutex_);
  w.batch = my_batch;  // <- writer指回向原来的writeBatch
  w.sync = options.sync;
  w.done = false;

  // 这里一开始用了一个局变量来拿锁
  // 在函数退出的时候，就不用手动放锁了
  MutexLock l(&mutex_);

  // 放到队列中，注意，这个时候，应该是位于队尾.
  // 放到队列中的是Writer，而不是WriteBatch
  // 这是因为Writer是一个与db的唯一锁相关的数据结构
  writers_.push_back(&w);
  // 如果还没有写入完成
  // 并且队列里面还有其他的item在等着被刷写下去
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  // 注意，从前面的while退出
  // 有两种情况
  // 1. 完成了。
  // 2. 位于队首
  if (w.done) {
    return w.status;
  }

  // 如果运行到这里，就只剩下一种情况，那就是当前线程的请求位于队首.
  // 其他线程的::Write()操作会被卡住

  // May temporarily unlock and wait.
  // 当这里在刷写的时候，由于会临时地释放锁，所以可能导致的情况是
  // 其他线程可能会拿到锁，然后往队列中往很多元素
  // 但是由于其他线程放置的元素并不是在队首，所以会被卡住
  // 然后当前这个线程切回来的时候，拿到锁之后，会把队列里面的所有的元素都一把写下去
  // 这里如果是需要延迟当前写，那么会sleep(1s)。然后会把锁放掉
  Status status = MakeRoomForWrite(my_batch == nullptr);

  // 这里拿到当前的最新的seq number.
  uint64_t last_sequence = versions_->LastSequence();
  // last_writer就是位于Queue队首的item.
  Writer* last_writer = &w;

  // 这里有两种情况status会失败，一种是bg_error_
  // 一种是wal log文件生成新的。
  // NOTE: my_batch == nullptr的时候
  // 是用触发强制compaction的
  if (status.ok() && my_batch != nullptr) {
    // nullptr batch is for compactions
    // 这里生成一个WriteBatch
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    // 经过BuildBatchGroup之后，last_writer指针的指向已经不再是w了。
    // updates 指向 WriteBatch* DBImpl::tmp_batch_

    // 打包之后，需要设置一个SN
    // 注意，只有当前的这个线程是拿到锁的，所以
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    // 注意，批量的写入之后，last_sn += count
    // Q: 中间的sn还用不用?
    last_sequence += WriteBatchInternal::Count(updates);

    // 接下来要把要写入的item追加到wal log并且更新到memtable.
    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock(); // <- 写磁盘的时候，又把锁放掉
      // 注意，这个时候放掉锁，只是可以让其他线程可以把item追加到writers_这个队列中
      // 除了前台调用::Write()的线程之外，其他读线程，以及后台压缩的线程也可能拿到锁。

      // 这里把内容append到wal文件里面
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));

      // 看一下是否遇到了sync error?
      bool sync_error = false;
      // 是否需要sync一把呢？
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }

      // 如果前面的wal的写入是ok的
      if (status.ok()) {
        // 这里把信息更新到skiplist里面
        // 注意：这个时候，虽然没有锁，但是其他的调用::Write()的线程
        // 是不会争用skiplist的。
        // 因为他们都还被卡在
        // while (!w.done && &w != writers_.front()) {
        // 这里呢。
        // 所以代码中，有趣的是mem_并没有被锁保护起来
        // 注意看一下声明:里面没有GUARDED_BY
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }

      // 重新开始拿到锁
      mutex_.Lock();
      // 如果前面的批量写出错了
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }

    // 如果更新是类内部的temp_batch_那么把已经写入的内容清理一下
    if (updates == tmp_batch_) tmp_batch_->Clear();

    // 判断，这个必须拿到锁了才能操作。
    // 更新SN
    // 有趣的是sn并不是在数据库里面。而是在version_set里面。
    versions_->SetLastSequence(last_sequence);
  }

  // 打包写入的item
  // 依次通知队列里面的每个元素
  while (true) {
    // 注意，队首元素不用通知
    Writer* ready = writers_.front();
    writers_.pop_front();

    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    // 打包好的里面的最后一个
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  // 如果队列不空
  // 那么这里唤醒它
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  // 到这里运行，肯定是拿到锁的
  mutex_.AssertHeld();
  // 当前线程是位于writers_这个queue的前面
  assert(!writers_.empty());

  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  // 拿到当前队首的写入的item的大小
  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // 这里是想把几个writer一起打包走
  // 但是如果原始的写是非常小的，那么也需要限制这个增长。
  // 因为如果打包得太大，可能会让批量的比较小的写入较慢。
  // NOTE: 个人觉得这里应该可以一起打包写。现在磁盘带宽一般不是问题
  // 都是iops比较少
  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  // 如果size并不是特别小，那么打包为1MB
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    // 如果size太小，那么这里直接打包写入弄得小一点
    max_size = size + (128<<10);
  }

  *last_writer = first;

  // 这里依次查看队列中的每个元素
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first" 跳过了当前线程指向的front()

  // 跳过了队首，然后开始依次遍历后面队列中的每个元素
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    // 如果后面的item需要sync
    // 或者说当前的队首元素需要sync
    // 跳出来
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    // 如果写入的batch非空
    // 这么一说还会有空的写入？
    if (w->batch != nullptr) {
      // 看一下有没有超出size
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      // 处理队道的batch
      // 这个判断最好是放到逻辑外面
      // 否则一直在for循环里面
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_; // temp_batch_是一个类内部的变量，并没有使用局变量或者得new一个.
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      // 追加到BATCH里面
      WriteBatchInternal::Append(result, w->batch);
    }
    // 更新last_writer
    *last_writer = w;
  }
  // 返回打包后的write batch
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
// 当前线程要处理的刚好是queue中的第一个元素.
// 大部分时候force = false.如果是正常写入的数据的话。
// force == false 表示 Do not force another compaction if have room
// 所以force表示是/否要强制一个新的compaction.
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  // 因为当前线程指向Queue中的第一个元素
  // 所以Q肯定不为空
  assert(!writers_.empty());

  // 如果force == false表示正常写入数据
  // 那么这个时候allow_delay就是true
  // 也就是说，如果是正常写入数据，那么这个时候是可以被delay的。
  bool allow_delay = !force;
  Status s;
  while (true/*延时一会退出，或者是compaction完成再退出*/) {
    // Case 1.
    // 看一下后台有没有出错，如果后台出错，那么就直接跳出
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    
    // Case 2.
    // 看一下是否需要延迟写入
    } else if (
        allow_delay /*如果是正常写数据，那么allow_delay = true*/ &&
        /*如果level0的文件数大于8个文件，那么就开始准备把磁盘的写入速度慢一点*/
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      // 注意当前这个线程中的元素位于队首
      // 这里让出锁之后
      // 其他元素会拿到的是队列，然后往队列中填入元素
      // [current_thread thread_b thread_c thread_d ....]
      // 但是thread_b thread_c thread_d都会被卡住，因为
      // ::Write()函数中，会检查两个条件，一个是w.done(), &w = q.front()
      // 这两个都不满足，所以只会放到队列中，然后被卡住。

      mutex_.Unlock();
      // 延迟写入的方式就是让度线程给后台的线程，比如后台线程可能需要做一些compaction.
      env_->SleepForMicroseconds(1000);
      // 延迟过一次了，所以这里不能再延迟写入了
      allow_delay = false;  // Do not delay a single write more than once
      // 重新拿回queue的锁
      mutex_.Lock();
    
    // Case 3.
    // 如果发现是正常的写入，注意这里并没有用allow_delay变量了
    // 并且发现skiplist里面的内存还是足够的
    // 那么直接退出
    // !force表示不强制compaction.
    } else if (!force /*也就是说，如果是正常的写入*/&&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;

    // Case 4.
    // 如果发现imm_不为空，那么这里等待一下后台线程的compaction.
    // 注意：imm_可以被直接访问，这是因为imm_是一个原子变量
    // 并不被mutex_上锁
    // **走到这里memtable必然已经满了**
    // 如果需要把imm_弄到磁盘上，这个时候就需要写磁盘什么的。
    // 这里就等一下。这里可能后台还会有任务在进行。
    // 等后面的压缩，或者imm_到磁盘
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      // 这里应该是确信imm_非空的时候，后台肯定有compaction的?
      // 由于现在持有mutex_锁，所以这里也可以检查一下
      // if (!background_compaction_scheduled_) {
      //    MaybeScheduledCompaction(); // 如果发现没有compaction那么这里trigger一把
      // }
      background_work_finished_signal_.Wait();

    // Case 5
    // **走到这里memtable内存空间必然满了**
    // **走到这里，imm_肯定已经被处理掉了**所以imm_肯定为空。
    // 如果当前的L0文件太多，已要需要停止写入了。
    // 那么这里也是需要等后台把任务完成。
    // 如果这里大于stop write的文件数目
    // 那么这里直接停下来，等待bg work finish
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      // 由于现在持有mutex_锁，所以这里也可以检查一下
      // if (!background_compaction_scheduled_) {
      //    MaybeScheduledCompaction(); // 如果发现没有compaction那么这里trigger一把
      // }
      background_work_finished_signal_.Wait();

    // Case 6.
    // **走到这里，memtable必然已经满了**
    // **走到这里, imm_肯定已经被写到磁盘了**，所以imm_肯定为空。
    // **走到这里，Level0的文件肯定是小于kL0_StopWritesTrigger的。也就是小于12
    // 其他情况: 如果有空间，前面已经返回了
    // 如果需要compaction，前面已经等待了
    // 那么这里就是：没有空间，也不需要compaction
    // 没有空间，那就意味着，内存写满了。那么就需要生成新的imm_.
    //   生成新的imm_，旧的imm_怎么办呢？前面不是会判断imm_么？
    //   如果imm_不为空，就不会跳到这里来。
    //   所以这里的情况就是imm_已经被刷到磁盘了。mm_的空间已经满了。
    // 注意这个LOG不是print出来的LOG。而是WAL日志，而是journal。
    // 不需要compaction，那就意味着可以直接生成LOG。
    // 那么就是生成新的LOG和新的memtable.
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      // 这里想切换到一个新的memtable并且触发compaction。把旧的数据合并掉。
      // 这里说的是当前还有没有LOG file在被compaction.
      // 由于versions_->NumLevelFiles(0) < config::kL0_StopWritesTrigger
      // 在这里必然成立，所以肯定是没有LOG文件在被compaction的。
      // PrevLogNumber指的是前面还有没有LOG文件在被合并。
      assert(versions_->PrevLogNumber() == 0);
      // 每次重新生成level 0的文件，就都新生成一个新的LOG文件
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      // 这里切换到了一个新的WAL LOG文件
      log_ = new log::Writer(lfile);

      // 这里把mem_切换到imm_，然后去trigger compaction.
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      // 这里生成新的skiplist.
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      // 这里只是trigger一下，把task扔到后台的线程里面
      MaybeScheduleCompaction();
    }
  }
  return s;
}

// 说是属性，实际上就是去拿当前db的状态
// 比如当前db每个层级有多少个文件
// sstable文件的情况
// 内存的使用情况
bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);

  // 这里就是看一下用户传进来的property里面是否有leveldb.开头
  // 如果没有，那么返回false.
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;

  // 如果有，那么移除prefix
  in.remove_prefix(prefix.size());

  // 去拿每个层级的文件数目
  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }

  // 去拿当前的状态
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;

  // 去拿sstable相关的信息
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  
  // 去拿内存的使用情况，这里应该是
  // 包含了
  // block_cache
  // mem_
  // imm_
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation

  // 这里只是简单的用一个锁，拿到current_ version的指针
  // 在使用version的时候，并不需要锁
  // 这是因为version这个类一旦生成，那么就成了只读的了。
  // 在使用的时候，只能是只读的
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  // 注意使用完之后，需要对version做unref
  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
// 既使是单个的Put，这里也是放到batch里面。
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

// 删除一个key
// leveldb这里就是把相应的类型定义为kDeleteType
// 然后在后面合并的时候，直接删除掉
Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

// 这里开始打开数据库
Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  // db指针，一开始设置为空
  // 注意这里传的是虚类的指针
  *dbptr = nullptr;

  // 根据需要来决定具体的实现
  DBImpl* impl = new DBImpl(options, dbname);

  // 先上锁
  impl->mutex_.Lock();

  VersionEdit edit;

  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;

  // NOTE: 这里代码比较绕，在Recover里面，如果发现数据库不存在
  // 那么会想办法创建一个新的出来。
  Status s = impl->Recover(&edit, &save_manifest);

  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }

  // 如果需要新建一个manifest
  if (s.ok() && save_manifest) {
    // prev_log_number_在恢复之后就没有什么用了
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    // 设置一个新的wal log序号
    edit.SetLogNumber(impl->logfile_number_);
    // 把这个修改反应到version_set_上
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    // 由于level 0生成了一些文件，可能是需要触发compaction.
    impl->MaybeScheduleCompaction();
  }
  // 为什么不在函数的最后再把锁释放了?
  // 由于是在open函数里面。前面的文件锁，应该会把其他的thread open挡住
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

// 这里就是去清除各种文件
Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
