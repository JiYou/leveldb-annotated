// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "util/testharness.h"

namespace leveldb {
namespace log {

// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string& partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

class LogTest {};

TEST(LogTest, Empty) {
    WritableFile *file = nullptr;
    Env *env = Env::Default();
    Status s = env->NewWritableFile("log_test_rw.log", &file);

    Writer *writer = new Writer(file);
    auto content = BigString("AAA", kBlockSize - kHeaderSize + 7);
    // A会占用第1个block的前半部分，并且会在第2个block
    // 占用开头的14byte.
    writer->AddRecord(Slice(content));
    content = BigString("BBB", kBlockSize - kHeaderSize - 7 - 7);
    writer->AddRecord(Slice(content));

    file->Flush();
    delete writer;
    file->Close();

    // Read out the full object.
    FILE *fp = fopen("log_test_rw.log", "r");
    if (NULL == fp) {
        std::cout << "Open file failed" << std::endl;
    } else {
        int c = EOF;
        int a_cnt = 0;
        int b_cnt = 0;
        int space_cnt = 0;
        while ((c=fgetc(fp)) != EOF) {
            //printf("%c", c);
            a_cnt += c == 'A';
            b_cnt += c == 'B';
            space_cnt += c == 0;
        }
        printf("A: %d 0: %d\n", a_cnt, space_cnt);
        printf("B: %d\n", b_cnt);
        fclose(fp);
        fp = NULL;
    }

    // test log reader.
    SequentialFile *rfile = nullptr;
    s = env->NewSequentialFile("log_test_rw.log", &rfile);
    Reader *reader = new Reader(rfile, nullptr, false, kBlockSize - 6);

    Slice record;
    std::string data;
    auto ret = reader->ReadRecord(&record, &data);
    std::cout << "ret = " << ret << std::endl;
    std::cout << "record.size() = " << record.size() << std::endl;
    std::cout << "data.length() = " << data.length() << std::endl;
    delete reader;
    //rfile->Close();
    delete rfile;
}

}  // namespace log
}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
