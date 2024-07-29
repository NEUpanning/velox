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
#include <getopt.h>
#include "hdfs/hdfs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/hidi/reader/HFileReader.h"
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
  const char* file = NULL;
  int iter = 1;
  int verbose = false;
  // hdfs related
  const char* krb5 = NULL;
  const char* conf = NULL;
  const char* host = "default";
  int port = 0;
} options;

void print_usage() {
  printf("velox_scan_hfile -s startKey -e endKey -f file\n"
      "  -s, --start          start key\n"
      "  -e, --end            end key\n"
      "  -f, --file           target file to scan\n"
      "  -i, --iter           number of iterators\n"
      "  -k, --krb5_file      the krb5.conf file's path\n"
      "  -c, --conf_file      the hdfs-site.xml's path\n"
      "  -h, --host           the service host\n"
      "  -p, --port           the service port\n"
      "  -v, --verbose        verbose output\n");
}

void parse_options(int argc, char *argv[]) {
  static struct option options_config[] = {
      {"start",          required_argument, 0,                's'},
      {"end",            required_argument, 0,                'e'},
      {"file",           required_argument, 0,                'f'},
      {"iter",           optional_argument, 0,                'i'},
      {"krb5_file",      required_argument, 0,                'k'},
      {"conf_file",      required_argument, 0,                'c'},
      {"host",           optional_argument, 0,                'h'},
      {"port",           optional_argument, 0,                'p'},
      {"verbose",        no_argument,       &options.verbose, 'v'},
      {0, 0,                                0, 0}
  };

  int c = 0;
  while (c >= 0) {
    int option_index;
    c = getopt_long(argc, argv, "s:e:f:i:k:c:h:p:v", options_config, &option_index);
    switch (c) {
      case 's':
        options.startKey = optarg;
        break;
      case 'e':
        options.endKey = optarg;
        break;
      case 'f':
        options.file = optarg;
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
      case 'v':
        options.verbose = true;
        break;
      default:
        break;
    }
  }

  if (options.file == NULL) {
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

/// velox_scan_hfile -f /velox/hidi_hfile_150M -s 99990210011536385292783706133 -e 99994810011490584736756756544
/// Scan 380873 Cells use 343 ms.
int main(int argc, char** argv) {
  parse_options(argc, argv);
  setenv("LIBHDFS3_CONF", options.conf, 1);
  setenv("KRB5_CONFIG", options.krb5, 1);

  Scan scan;
  if (options.startKey) {
    scan.startKey = options.startKey;
  }
  if (options.endKey) {
    scan.endKey = options.endKey;
  }
  filesystems::registerLocalFileSystem();
  memory::MemoryManager::initialize({});
  auto pool = memory::memoryManager()->addLeafPool();
  CompressionOptions compressOpts;
  compressOpts.format.lz4_lzo.isHadoopFrameFormat = true;
  compressOpts.format.lz4_lzo.strategy = 12; // lzo1x

  hdfsFS fs = nullptr;
  if (options.conf) {
    fs = hdfsConnect(options.host, options.port);
    if (!fs) {
      std::cout << "cannot connect hdfs.\n";
      return -1;
    }
  }

  std::string filePath{options.file};
  std::shared_ptr<ReadFile> readFile;
  if (fs) {
    readFile = std::static_pointer_cast<ReadFile>(std::make_shared<HdfsReadFile>(fs, filePath));
  } else {
    readFile = std::static_pointer_cast<ReadFile>(std::make_shared<LocalReadFile>(filePath));
  }
  struct timespec startTime, endTime, d;
  clock_gettime(CLOCK_MONOTONIC, &startTime);

  uint64_t entryCount = 0;
  uint64_t deleteCount = 0;
  for (int i = 0; i < options.iter; i++) {
    HFileReader reader(readFile, *pool, compressOpts);
    if (reader.seek(scan)) {
      while (reader.next()) {
        auto& cell = reader.getCell();
        if (cell.type != 4/*Put*/) {
          deleteCount++;
        }
        if (options.verbose) {
          if (entryCount % 1000 == 0) {
            printf("[rowkey] %s\n", std::string(cell.rowkey, cell.rowkeyLen).c_str());
          }
        }
        entryCount++;
      }
    } else {
      printf("do seek failed!\n");
    }
  }

  clock_gettime(CLOCK_MONOTONIC, &endTime);
  d = timespec_diff(startTime, endTime);
  long cost = d.tv_sec * 1000 + d.tv_nsec / 1000000.0;
  std::cout << "Scan " << entryCount/options.iter << " Cells use "
            << cost/options.iter << " ms, " << deleteCount/options.iter
            << " Cells with DELETE type.\n";

  if (fs && hdfsDisconnect(fs)) {
    std::cout << "disconnect from hdfs failed.\n";
  }
  return 0;
}
