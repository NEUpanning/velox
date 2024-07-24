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

#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/hidi/reader/KVHeap.h"
#include "velox/dwio/hidi/reader/HidiSerde.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;

namespace facebook::velox::hidi {

class HidiReader : public RowReader {
 public:
  static constexpr int8_t KPutType = 4;

  HidiReader(
      const std::vector<std::shared_ptr<ReadFile>>& files,
      const dwio::common::ReaderOptions& readerOpts,
      const dwio::common::RowReaderOptions& rowReaderOpts,
      bool compactValues = true);

  ~HidiReader() override = default;
  bool seek(const Scan& scan);

  void updateRuntimeStats(RuntimeStatistics& stats) const override {
    // nothing to do yet
  }

  /// HidiReader don't need this
  int64_t nextRowNumber() override {
    return kAtEnd;
  }

  /// HidiReader don't need this
  int64_t nextReadSize(uint64_t size) override {
    return kAtEnd;
  }

  /// HidiReader don't support filter push down yet
  void resetFilterCaches() override {}

  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const Mutation* = nullptr) override;

  std::optional<size_t> estimatedRowSize() const override;

 private:
  /**
   * Fetch next Cell and add to the output vector
   * @param outputVecs Vector to save value with
   * @return whether there are more Cell to fetch
   */
  template <bool kReadAll, bool kCompactValue>
  bool next(const std::vector<VectorPtr>& outputVecs,
      const std::unordered_map<std::string_view, uint32_t>& outputMap);

  void prepareForReuse(RowVector* rowVector, uint64_t size) const;

  inline void checkSchema() const;

  template <bool kCompactValue>
  inline bool shouldSkip(const Cell& cell) const {
    bool res = compareTo(curRowkey_->data(), curRowkey_->size(),
        cell.rowkey, cell.rowkeyLen) == 0;
    if constexpr (kCompactValue) {
      return res;
    } else {
      return res && compareTo(curCol_->data(), curCol_->size(),
          cell.column, cell.columnLen) == 0;
    }
  }

 private:
  CompressionOptions options_;
  const dwio::common::ReaderOptions& readerOpts_;
  const dwio::common::RowReaderOptions& rowReaderOpts_;
  const std::shared_ptr<ColumnSelector> selector_;
  const HidiSerde serde_;
  DeserializeContext serdeContext_;
  uint64_t totalCount_;
  uint64_t totalSizes_;
  bool reachEnd_;
  const bool compactValues_;
  bool readAll_;
  int64_t schemaSize_;
  char* fileSchema_;
  std::unique_ptr<DataBuffer<char>> curRowkey_;
  std::unique_ptr<DataBuffer<char>> curCol_;
  std::vector<std::shared_ptr<HFileReader>> hfileReaders;
  BinaryHeap<HFileReader*, HFileReaderComparator> heap;
};

class HidiReaderFactory : public ReaderFactory {
 public:
  HidiReaderFactory() : ReaderFactory(FileFormat::HIDI) {}

  std::unique_ptr<Reader> createReader(
      std::unique_ptr<BufferedInput> input,
      const dwio::common::ReaderOptions& options) override {
    return nullptr; // HidiInputSplit only need RowReader
  }

  std::unique_ptr<RowReader> createRowReader(
      const std::vector<std::shared_ptr<ReadFile>>& files,
      const dwio::common::ReaderOptions& readerOpts,
      const dwio::common::RowReaderOptions& rowReaderOpts,
      const std::unordered_map<std::string, std::string>& splitInfo) {
    bool compactValues = true;
    auto target = splitInfo.find("compactValues");
    if (target != splitInfo.end() && target->second == "false") {
      compactValues = false;
    }
    auto hidiReader = std::make_unique<HidiReader>(
        files, readerOpts, rowReaderOpts, compactValues);
    Scan scan;
    target = splitInfo.find("startRow");
    if (target != splitInfo.end()) {
      scan.startKey = target->second;
    }
    target = splitInfo.find("stopRow");
    if (target != splitInfo.end()) {
      scan.endKey = target->second;
    }
    if (hidiReader->seek(scan)) {
      LOG(INFO) << "Construct HidiReader with " << files.size()
                << " HFiles, compactValues is " << (compactValues ? "true" : "false")
                << ", startKey is '" << scan.startKey
                << "', endKey is '" << scan.endKey << "'.";
    }
    return hidiReader;
  }
};

void registerHidiReaderFactory();
void unregisterHidiReaderFactory();

} // namespace facebook::velox::hidi
