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

#include "velox/connectors/hive/HidiSplitReader.h"
#include "velox/dwio/hidi/reader/HidiReader.h"

namespace facebook::velox::connector::hive::hidi {

HidiSplitReader::HidiSplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
    const std::shared_ptr<const HiveTableHandle>& hiveTableHandle,
    const std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>*
        partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    FileHandleFactory* const fileHandleFactory,
    folly::Executor* executor,
    const std::shared_ptr<common::ScanSpec>& scanSpec)
    : SplitReader(
          hiveSplit,
          hiveTableHandle,
          partitionKeys,
          connectorQueryCtx,
          hiveConfig,
          readerOutputType,
          ioStats,
          fileHandleFactory,
          executor,
          scanSpec),
      hasConstantColumn_(false) {}

void HidiSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats,
    const std::shared_ptr<HiveColumnHandle>& rowIndexColumn) {
  std::vector<std::shared_ptr<ReadFile>> files;
  // get File list from HiveConnectorSplit
  folly::dynamic splitInfo = folly::parseJson(hiveSplit_->filePath);
  VELOX_CHECK(splitInfo.get_ptr("dir"), "HidiSplit should have dir info!");
  VELOX_CHECK(splitInfo.get_ptr("files"), "HidiSplit should have files info!");
  std::string dir = splitInfo["dir"].asString();
  dir += "/";
  for (const auto& file : splitInfo["files"]) {
    try {
      files.emplace_back(fileHandleFactory_->generate(dir + file.asString())->file);
    } catch (const VeloxException& e) {
      // Corresponding to HidiKeyValueScanner's rebuildReader
      LOG(WARNING) << e.message();
      std::string trashFile = dir + ".HIDI_TRASH/" + file.asString();
      try {
        files.emplace_back(fileHandleFactory_->generate(trashFile)->file);
      } catch (const VeloxException& e2) {
        LOG(WARNING) << e2.message();
        const size_t last_slash_idx = splitInfo["dir"].asString().rfind('/');
        std::string parentDir = dir.substr(0, last_slash_idx);
        trashFile = parentDir + "/.HIDI_TRASH/" + file.asString();
        files.emplace_back(fileHandleFactory_->generate(trashFile)->file);
      }
    }
  }

  // judge table type
  bool compactValues = true;
  auto& customInfo = hiveSplit_->customSplitInfo;
  auto target = customInfo.find("compactValues");
  if (target != customInfo.end()) {
    if (target->second == "false") {
      compactValues = false;
    }
  } else {
    auto tableParam = hiveTableHandle_->tableParameters().find(
        dwio::common::TableParameter::kCompactValues);
    if (tableParam != hiveTableHandle_->tableParameters().end() &&
        tableParam->second == "false") {
      compactValues = false;
    }
  }

  // setup ColumnSelector
  baseRowReaderOpts_.setScanSpec(scanSpec_);
  auto& fileType = baseReaderOpts_.fileSchema();
  auto columnTypes = adaptColumns(fileType, fileType);
  auto rowType = ROW(std::vector<std::string>(fileType->names()), std::move(columnTypes));
  LOG(WARNING) << "[AdaptedSchema] " << rowType->toString();
  std::vector<std::string> columnNames;
  for (auto& spec : scanSpec_->children()) {
    if (!spec->isConstant()) {
      columnNames.push_back(spec->fieldName());
    } else {
      LOG(WARNING) << "column " << spec->fieldName() << " is constant.";
      hasConstantColumn_ = true;
    }
  }
  std::shared_ptr<dwio::common::ColumnSelector> selector;
  if (columnNames.empty()) {
    static const RowTypePtr kEmpty{ROW({}, {})};
    selector = std::make_shared<dwio::common::ColumnSelector>(kEmpty);
  } else {
    selector = std::make_shared<dwio::common::ColumnSelector>(rowType, columnNames);
  }
  baseRowReaderOpts_.select(selector);

  // create RowReader
  auto hidiReader = std::make_unique<velox::hidi::HidiReader>(
      files, baseReaderOpts_, baseRowReaderOpts_, compactValues);
  velox::hidi::Scan scan;
  if (splitInfo.get_ptr("startRow")) {
    scan.startKey = splitInfo["startRow"].asString();
  }
  if (splitInfo.get_ptr("stopRow")) {
    scan.endKey = splitInfo["stopRow"].asString();
  }
  if (hidiReader->seek(scan)) {
    LOG(WARNING) << "Construct HidiReader with " << files.size()
                 << " HFiles, compactValues is " << (compactValues ? "true" : "false")
                 << ", startKey is '" << scan.startKey
                 << "', endKey is '" << scan.endKey << "'.";
  }
  baseRowReader_ = std::move(hidiReader);
  VELOX_CHECK(baseRowReader_ != nullptr, "Create row reader failed!");
  emptySplit_ = false;
}

uint64_t HidiSplitReader::next(uint64_t size, VectorPtr& output) {
  uint64_t numValues = baseRowReader_->next(size, output);
  if (hasConstantColumn_) {
    auto rowVector = std::dynamic_pointer_cast<RowVector>(output);
    auto& childSpecs = scanSpec_->children();
    for (auto& childSpec : childSpecs) {
      if (childSpec->isConstant()) {
        auto channel = childSpec->channel();
        rowVector->childAt(channel) = BaseVector::wrapInConstant(
            numValues, 0, childSpec->constantValue());
      }
    }
  }
  return numValues;
}

} // namespace facebook::velox::connector::hive::hidi
