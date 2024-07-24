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

#pragma once

#include "velox/dwio/hidi/reader/Compression.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;

namespace facebook::velox::hidi {

struct Scan {
  std::string startKey;
  std::string endKey;
  Scan(const std::string& start = "", const std::string& end = "");
  bool fullScan() const {
    return startKey.empty() && endKey.empty();
  }
};

enum class MvccType {
  MYSQLV2, SEQID_BASE
};

struct MysqlV2MvccInfo {
  int64_t executeTime;
  int64_t sourceIpGlobalId;
  int32_t binlogFileIndex;
  int64_t binlogFileOffset;
  int64_t changeSetLineSeq;
  int compareTo(const MysqlV2MvccInfo& other) const;
};

struct Cell {
  int32_t rowkeyLen;
  const char* rowkey;

  int32_t valLen;
  const char* value;

  int8_t familyLen;
  const char* family;

  int8_t columnLen;
  const char* column;

  int64_t timestamp;
  int8_t type;

  int16_t tagsLen;
  const char* tags;

  int8_t mvccLen;
  const char* mvcc;
  MvccType mvccType;

  bool getMysqlV2MvccInfo(MysqlV2MvccInfo& mvccInfo) const;
  long getSequeceId() const;
};

class HFileReader {
 public:
  static constexpr int32_t kTrailerSize = 4096;
  static constexpr int8_t kTrailerMagicLen = 8;
  static constexpr int32_t kTrailerProtoLen =
      kTrailerSize - kTrailerMagicLen - 4/*VERSION_LEN*/ - 12/*PADDING*/;
  static constexpr int32_t kFooterSize = kTrailerSize + 4096/*LOAD_ON_OPEN*/;
  static constexpr int8_t KHeaderSize = 33;
  static constexpr int8_t KRowKeyOffset = 2;
  static constexpr int8_t KCompressedSizeOff = 8;
  static constexpr int8_t KUncompressedSizeOff = 12;
  static constexpr int8_t KRowInfraSize = 12;
  static constexpr int8_t KTagLenSize = 2;
  static constexpr int8_t KMvccTagType = 127;
  static constexpr std::string_view kMvccType = "mvcc_type";
  static constexpr std::string_view kSeqBaseMvcc = "SEQID_BASE";
  static constexpr std::string_view kColomnTypes = "columns.types";
  static constexpr std::string_view kDataBlock = "DATABLK*";
  static constexpr std::string_view kLeafIndex = "IDXLEAF2";
  static constexpr std::string_view kMidIndex = "IDXINTE2";

  HFileReader(
      std::shared_ptr<ReadFile> file,
      memory::MemoryPool& pool,
      CompressionOptions& options,
      std::shared_ptr<DataBuffer<char>> buffer = nullptr);

  bool seek(const Scan& scan);
  bool next();

  uint64_t getEntryCount() const;
  uint64_t getTotalSize() const;

  const Cell& getCell() const {
    return cell_;
  }

  const std::string& getFileName() const {
    return fileName_;
  }

  const std::string& getFileSchema() const {
    return fileSchema_;
  }

 private:
  bool loadFirstBlock();

  void loadDataBlock(
      int64_t& offset, int32_t compressedSize, int32_t uncompressedSize);

  int locateNonRootIndexEntry(const char* blockData) const;

  inline bool resetBlock();

  inline void decompresseBlock(int32_t compressedSize, int32_t uncompressedSize);

  inline void reset() {
    curBlockSize_ = 0;
    curBlockIndex_ = 0;
    startOffset_ = -1;
  }

  // reference to org.apache.hadoop.hbase.io.compress.Compression.Algorithm
  inline CompressionKind getCompressionKind(int32_t codec) {
    switch (codec) {
      case 0: {
        return CompressionKind_LZO;
      }
      case 1: {
        return CompressionKind_ZLIB;
      }
      case 2: {
        return CompressionKind_NONE;
      }
      case 3: {
        return CompressionKind_SNAPPY;
      }
      case 4: {
        return CompressionKind_LZ4;
      }
      case 6: {
        return CompressionKind_ZSTD;
      }
      default: {
        DWIO_RAISE("Unsupported Compression Type : {}", codec);
      }
    }
  }

 private:
  memory::MemoryPool& pool_;
  CompressionOptions& options_;
  std::shared_ptr<ReadFileInputStream> input_;
  uint64_t fileSize_;
  std::string fileName_;
  int32_t curBlockSize_;
  int32_t curBlockIndex_;
  int64_t startOffset_;
  int64_t endOffset_;
  uint64_t rootIndexOffset_;
  uint64_t entryCount_;
  uint64_t totalSizes_;
  bool indexMultiLevel_;
  // no need to compare end key between startOffset_ and endOffset_
  bool needCompareEndkey_;
  bool needCompareStartkey_;
  CompressionKind compressionKind_;
  std::string startKey_;
  std::string endKey_;
  std::string fileSchema_;
  std::unique_ptr<DataBuffer<char>> blockBuffer;
  std::shared_ptr<DataBuffer<char>> tempBuffer;
  Cell cell_;
  // next block header info
  bool nextDataBlock_;
  int32_t nextCompressedSize_;
  int32_t nextUncompressedSize_;
};

} // namespace facebook::velox::hidi
