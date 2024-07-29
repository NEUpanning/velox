/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/init/Init.h>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include "hdfs/hdfs.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/hidi/reader/HidiReader.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/BaseVector.h"
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsReadFile.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::hidi;

struct {
  const char* startKey = NULL;
  const char* endKey = NULL;
  const char* files = NULL;
  const char* type = NULL;
  const char* res = NULL;
  int iter = 1;
  int verbose = false;
  // hdfs related
  const char* krb5 = NULL;
  const char* conf = NULL;
  const char* host = "default";
  int port = 0;
  int max = 100;
} options;

void print_usage() {
  printf("velox_scan_hfiles -s startKey -e endKey -f files -t schema\n"
      "  -s, --start          start key\n"
      "  -e, --end            end key\n"
      "  -f, --files          target files to scan, split with ','\n"
      "  -t, --type           target file's schema\n"
      "  -r, --res            target column to select\n"
      "  -i, --iter           number of iterators\n"
      "  -k, --krb5_file      the krb5.conf file's path\n"
      "  -c, --conf_file      the hdfs-site.xml's path\n"
      "  -h, --host           the service host\n"
      "  -p, --port           the service port\n"
      "  -m, --max_files      max hfiles to scan\n"
      "  -v, --verbose        verbose output\n");
}

void parse_options(int argc, char *argv[]) {
  static struct option options_config[] = {
      {"start",          required_argument, 0,                's'},
      {"end",            required_argument, 0,                'e'},
      {"files",          required_argument, 0,                'f'},
      {"type",           required_argument, 0,                't'},
      {"res",            required_argument, 0,                'r'},
      {"iter",           optional_argument, 0,                'i'},
      {"krb5_file",      required_argument, 0,                'k'},
      {"conf_file",      required_argument, 0,                'c'},
      {"host",           optional_argument, 0,                'h'},
      {"port",           optional_argument, 0,                'p'},
      {"max_files",      optional_argument, 0,                'm'},
      {"verbose",        no_argument,       &options.verbose, 'v'},
      {0, 0,                                0, 0}
  };

  int c = 0;
  while (c >= 0) {
    int option_index;
    c = getopt_long(argc, argv, "s:e:f:t:r:i:k:c:h:p:m:v", options_config, &option_index);
    switch (c) {
      case 's':
        options.startKey = optarg;
        break;
      case 'e':
        options.endKey = optarg;
        break;
      case 'f':
        options.files = optarg;
        break;
      case 't':
        options.type = optarg;
        break;
      case 'r':
        options.res = optarg;
        break;
      case 'i':
        options.iter = atoi(optarg);
        break;
      case 'k':
        options.krb5 = optarg;
        break;
      case 'c':
        options.conf = optarg;
        break;
      case 'h':
        options.host = optarg;
        break;
      case 'p':
        options.port = atoi(optarg);
        break;
      case 'm':
        options.max = atoi(optarg);
        break;
      case 'v':
        options.verbose = true;
        break;
      default:
        break;
    }
  }

  if (options.files == NULL || options.type == NULL) {
    print_usage();
    exit(1);
  }
}

timespec timespec_diff(timespec start, timespec end) {
  timespec temp;
  if ((end.tv_nsec - start.tv_nsec) < 0) {
    temp.tv_sec = end.tv_sec - start.tv_sec - 1;
    temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
  } else {
    temp.tv_sec = end.tv_sec - start.tv_sec;
    temp.tv_nsec = end.tv_nsec - start.tv_nsec;
  }
  return temp;
}

void getTargetFiles(std::vector<std::shared_ptr<ReadFile>>& targetFiles, hdfsFS fs) {
  std::ifstream filelist(options.files);
  std::string file;
  while (std::getline(filelist, file)) {
    std::shared_ptr<ReadFile> readFile;
    if (fs) {
      readFile = std::static_pointer_cast<ReadFile>(std::make_shared<HdfsReadFile>(fs, file));
    } else {
      readFile = std::static_pointer_cast<ReadFile>(std::make_shared<LocalReadFile>(file));
    }
    targetFiles.emplace_back(readFile);
    if (targetFiles.size() >= options.max) {
      break;
    }
  }
}

int main(int argc, char** argv) {
  parse_options(argc, argv);
  setenv("LIBHDFS3_CONF", options.conf, 1);
  setenv("KRB5_CONFIG", options.krb5, 1);

  filesystems::registerLocalFileSystem();
  memory::MemoryManager::initialize({});
  auto pool = memory::memoryManager()->addLeafPool();

  hdfsFS fs = nullptr;
  if (options.conf) {
    fs = hdfsConnect(options.host, options.port);
    if (!fs) {
      std::cout << "cannot connect hdfs.\n";
      return -1;
    }
  }

  Scan scan;
  if (options.startKey) {
    scan.startKey = options.startKey;
  }
  if (options.endKey) {
    scan.endKey = options.endKey;
  }

  std::vector<std::shared_ptr<ReadFile>> targetFiles;
  getTargetFiles(targetFiles, fs);

  struct timespec startTime, endTime, d;
  clock_gettime(CLOCK_MONOTONIC, &startTime);
  int entryCount = 0;
  int batchCount = 100;

  auto type = asRowType(type::fbhive::HiveTypeParser().parse(options.type));
  dwio::common::ReaderOptions readerOpts(pool.get());
  dwio::common::RowReaderOptions rowReaderOpts;
  if (options.res) {
    std::vector<uint64_t> nodes = {1};
    rowReaderOpts.select(std::make_shared<ColumnSelector>(type, nodes, true));
  } else {
    rowReaderOpts.select(std::make_shared<ColumnSelector>(type));
  }
  readerOpts.setFileFormat(FileFormat::HIDI);
  readerOpts.setFileSchema(type);

  for (int i = 0; i < options.iter; i++) {
    HidiReader reader(targetFiles, readerOpts, rowReaderOpts, true/*compactValues*/);
    if (reader.seek(scan)) {
      int readCount = batchCount;
      auto vector = BaseVector::create(type, 0, pool.get());
      while (readCount == batchCount) {
        readCount = reader.next(batchCount, vector, nullptr);
        if (options.verbose) {
          printf("[reads] %d, [bytes] %ld, [size] %d\n",
              readCount, vector->as<RowVector>()->estimateFlatSize(), vector->size());
        }
        entryCount += readCount;
      }
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &endTime);
  d = timespec_diff(startTime, endTime);
  long cost = d.tv_sec * 1000 + d.tv_nsec / 1000000.0;
  std::cout << "Scan " << entryCount/options.iter << " Rows with "
            << targetFiles.size() << " files use " << cost/options.iter << " ms.\n";

  if (fs && hdfsDisconnect(fs)) {
    std::cout << "disconnect from hdfs failed.\n";
  }
  return 0;
}

/*
velox_scan_hfiles -f /tmp/hfiles.txt -t "struct<id:BIGINT,sub_trade_id:STRING,
   trade_id:STRING,amount:BIGINT,status:INT,create_time:STRING,update_time:STRING,
   pay_business_type:INT,adapter_charge_id:STRING,channel_type:INT,remark:STRING,
   ext:STRING,sub_business_id:STRING,trade_time:STRING,error_code:STRING,pay_type:STRING,
   pay_type_txt:STRING,mt_userid:STRING,user_id:STRING,_update_timestamp:STRING>"
   -s 7777 -e 9999
Scan 9449408 Cells with 10 files use 14431 ms.
*/
