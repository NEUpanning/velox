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

#include "velox/dwio/hidi/reader/HidiReader.h"

namespace facebook::velox::hidi {

HidiReader::HidiReader(
    const std::vector<std::shared_ptr<ReadFile>>& files,
    const dwio::common::ReaderOptions& readerOpts,
    const dwio::common::RowReaderOptions& rowReaderOpts,
    bool compactValues)
    : readerOpts_(readerOpts),
      rowReaderOpts_(rowReaderOpts),
      selector_(rowReaderOpts.getSelector()),
      serde_(selector_, readerOpts.fileSchema()),
      totalCount_(0), totalSizes_(0), reachEnd_(false),
      compactValues_(compactValues), schemaSize_(0) {
  options_.format.lz4_lzo.isHadoopFrameFormat = true;
  options_.format.lz4_lzo.strategy = 12; // lzo1x
  auto& pool = readerOpts.memoryPool();
  curRowkey_ = std::make_unique<DataBuffer<char>>(pool, 512);
  curCol_ = std::make_unique<DataBuffer<char>>(pool, 512);
  readAll_ = (!selector_ || selector_->shouldReadAll());
  auto buffer = std::make_shared<DataBuffer<char>>(pool, HFileReader::KHeaderSize);
  for (auto& file : files) {
    auto reader = std::make_shared<HFileReader>(file, pool, options_, buffer);
    hfileReaders.emplace_back(reader);
  }
}

std::optional<size_t> HidiReader::estimatedRowSize() const {
  return !totalCount_ ? 0 : totalSizes_ / totalCount_;
}

void HidiReader::checkSchema() const {
  if (schemaSize_ > 0) {
    LOG(WARNING) << "[FileSchema] " << std::string(fileSchema_);
  }
  if (readerOpts_.fileSchema()) {
    LOG(WARNING) << "[TableSchema] " << readerOpts_.fileSchema()->toString();
  }
  if (!readAll_) {
    LOG(WARNING) << "[OutputSchema] " << selector_->buildSelected()->type()->toString();
  }
}

// Add custom prepareForReuse due to
// BaseVector::prepareForReuse(RowVectorPtr, size) will zero children's size
void HidiReader::prepareForReuse(RowVector* rowVector, uint64_t size) const {
  for (auto& child : rowVector->children()) {
    if (child->typeKind() == TypeKind::ROW) {
      prepareForReuse(child->as<RowVector>(), size);
    } else {
      BaseVector::prepareForReuse(child, size);
    }
  }
  rowVector->resize(size);
}

template <bool kReadAll, bool kCompactValue>
bool HidiReader::next(const std::vector<VectorPtr>& outputVecs,
    const std::unordered_map<std::string_view, uint32_t>& outputMap) {
  bool skip = true;
  while (skip) {
    if (heap.empty()) {
      return false;
    }
    HFileReader* topReader = heap.top();
    auto& cell = topReader->getCell();
    skip = shouldSkip<kCompactValue>(cell);
    if (!skip) {
      curRowkey_->append(0, cell.rowkey, cell.rowkeyLen);
      if constexpr (!kCompactValue) {
        curCol_->append(0, cell.column, cell.columnLen);
      }
      if (cell.type != KPutType) {
        skip = true;
      } else {
        serdeContext_.resetBuffer(cell.value, cell.valLen);
        if constexpr (kCompactValue) {
          serde_.deserializeRow<kReadAll>(serdeContext_, outputVecs);
        } else { // split value case
          std::string_view colName(cell.column, cell.columnLen);
          DWIO_ENSURE(!colName.empty(), "Column name can't be empty.");
          auto& node = selector_->findNode(colName);
          if (node->shouldRead()) {
            serdeContext_.colIdx = (uint32_t) node->getId();
            serdeContext_.colType = const_cast<Type*>(node->getDataType().get());
            auto outIdx = outputMap.find(colName)->second;
            serdeContext_.outVec = outputVecs[outIdx].get();
            if (serdeContext_.buff[serdeContext_.bufIdx++] != 0) {
              serde_.deserializeColumn<kReadAll>(serdeContext_);
              DWIO_ENSURE(!serdeContext_.hasRemaining(), "Column has more data to read.");
            } else { // value is NULL
              serdeContext_.outVec->setNull(serdeContext_.rowIdx, true);
            }
          }
        }
      }
    }
    if (topReader->next()) {
      heap.replace_top(topReader);
    } else {
      heap.pop();
    }
  }
  return true;
}

uint64_t HidiReader::next(
    uint64_t size, VectorPtr& result, const Mutation* mutation) {
  if (reachEnd_ || !size) {
    return 0;
  }

  auto rowVector = result->as<RowVector>();
  prepareForReuse(rowVector, size);
  serdeContext_.reset();

  // sort output vector by colId
  auto outputType = asRowType(rowVector->type());
  auto dataType = readerOpts_.fileSchema();
  auto& childNames = dataType->names();
  std::vector<VectorPtr> outputVecs;
  std::unordered_map<std::string_view, uint32_t> outputMap;
  uint32_t index = 0;
  for (uint32_t childIdx = 0; childIdx < dataType->size(); childIdx++) {
    auto outIdx = outputType->getChildIdxIfExists(childNames[childIdx]);
    if (outIdx) {
      outputVecs.emplace_back(rowVector->childAt(*outIdx));
      outputMap.emplace(std::string_view(childNames[childIdx]), index++);
    }
  }

  // use template to avoid branch prediction in while loop
  if (compactValues_) {
    if (readAll_) {
      bool hasMore = next<true, true>(outputVecs, outputMap);
      while (hasMore && ++serdeContext_.rowIdx < size) {
        hasMore = next<true, true>(outputVecs, outputMap);
      }
    } else {
      bool hasMore = next<false, true>(outputVecs, outputMap);
      while (hasMore && ++serdeContext_.rowIdx < size) {
        hasMore = next<false, true>(outputVecs, outputMap);
      }
    }
  } else { // split value case
    if (readAll_) {
      bool hasMore = next<true, false>(outputVecs, outputMap);
      while (hasMore && serdeContext_.rowIdx < size) {
        for (uint32_t i = 1; i < dataType->size(); i++) {
          next<true, false>(outputVecs, outputMap);
        }
        serdeContext_.rowIdx++;
        hasMore = next<true, false>(outputVecs, outputMap);
      }
    } else {
      bool hasMore = next<false, false>(outputVecs, outputMap);
      while (hasMore && serdeContext_.rowIdx < size) {
        for (uint32_t i = 1; i < dataType->size(); i++) {
          next<false, false>(outputVecs, outputMap);
        }
        serdeContext_.rowIdx++;
        hasMore = next<false, false>(outputVecs, outputMap);
      }
    }
  }

  // last batch
  if (serdeContext_.rowIdx && serdeContext_.rowIdx < size) {
    // can't use RowVector::resize, since it does't update children's size
    result = rowVector->slice(0, serdeContext_.rowIdx);
    reachEnd_ = true;
  }
  return serdeContext_.rowIdx;
}

bool HidiReader::seek(const Scan& scan) {
  bool res = true;
  bool mismatch = false;
  for (auto& reader : hfileReaders) {
    if (!reader->seek(scan) || !reader->next()) {
      LOG(WARNING) << "Skip " << reader->getFileName()
                   << ", since it does't contain the key rang("
                   << scan.startKey << " ~ " << scan.endKey << ")";
      continue;
    }
    if (!mismatch) {
      if (reader->getFileSchema().empty()) {
        if (!schemaSize_) {
          schemaSize_ = -1;
        } else if (schemaSize_ != -1) {
          mismatch = true;
          LOG(WARNING) << reader->getFileName()
                       << "'s schema mismatch with other HFile.";
        }
      } else {
        int64_t schemaSize = reader->getFileSchema().size();
        const char* fileSchema = reader->getFileSchema().data();
        if (!schemaSize_) {
          schemaSize_ = schemaSize;
          fileSchema_ = const_cast<char*>(fileSchema);
        } else if (schemaSize_ != schemaSize ||
            std::memcmp(fileSchema_, fileSchema, schemaSize_) != 0) {
          mismatch = true;
          LOG(WARNING) << reader->getFileName()
                       << "'s schema mismatch with other HFile.";
        }
      }
    }
    totalCount_ += reader->getEntryCount();
    totalSizes_ + reader->getTotalSize();
    heap.push(reader.get());
  }
  checkSchema();
  return heap.size() > 0;
}
} // namespace facebook::velox::hidi
