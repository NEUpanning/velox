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

#include "velox/dwio/hidi/reader/HFileReader.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;

namespace facebook::velox::hidi {

struct DeserializeContext {
  DeserializeContext() : bufIdx(0), colIdx(0), rowIdx(0) {}

  DeserializeContext(const char* buffer, int32_t size)
      : buff(const_cast<char*>(buffer)), bufSize(size),
        bufIdx(0), colIdx(0), rowIdx(0) {}

  inline bool hasRemaining() {
    return bufIdx < bufSize;
  }

  inline void reset() {
    rowIdx = 0;
    bufIdx = 0;
    colIdx = 0;
    outVec = nullptr;
    colType = nullptr;
  }

  inline void resetBuffer(const char* buffer, int32_t size) {
    buff = const_cast<char *>(buffer);
    bufSize = size;
    bufIdx = 0;
    colIdx = 0;
  }

  char* buff;
  int32_t bufSize;
  int32_t bufIdx;
  uint32_t colIdx;
  BaseVector* outVec;
  Type* colType;
  int32_t rowIdx;
};

class HidiSerde {
 public:
  static constexpr int32_t kArrayInitSize = 10000;

  HidiSerde(
      const std::shared_ptr<ColumnSelector> selector,
      const std::shared_ptr<const RowType> schema);
  ~HidiSerde() = default;

  /// Corresponding to HidiStructObject's deserialize
  template <bool kReadAll>
  void deserializeRow(
      DeserializeContext& context,
      const std::vector<VectorPtr>& outputVecs,
      bool earlyResponse = true) const;

  template <bool kReadAll>
  bool deserializeColumn(DeserializeContext& context) const;

 private:
  /// Corresponding to HidiSerde's serializeHiveDecimal
  template <typename T>
  inline T deserializeDecimal(
      DeserializeContext& context, int32_t targetScale) const;

  template <bool kReadAll>
  inline bool deserializeArray(DeserializeContext& context) const;

  template <bool kReadAll>
  inline bool deserializeMap(DeserializeContext& context) const;

  /// Corresponding to SelectiveColumnReader::getFlatValues
  template <typename T, typename TVector>
  inline void saveScalarValue(DeserializeContext& context, T val) const {
    if constexpr (std::is_same_v<T, TVector>) {
      context.outVec->as<FlatVector<TVector>>()->set(context.rowIdx, val);
    } else if constexpr (sizeof(TVector) > sizeof(T)) {
      TVector value;
      std::memcpy(&value, &val, sizeof val);
      context.outVec->as<FlatVector<TVector>>()->set(context.rowIdx, value);
    } else {
      TVector value = *reinterpret_cast<TVector*>(&val);
      context.outVec->as<FlatVector<TVector>>()->set(context.rowIdx, value);
    }
  }

  template <bool kReadAll, typename T>
  inline bool saveToVector(DeserializeContext& context, T val) const {
    bool res = false;
    if constexpr (kReadAll) {
      context.outVec->as<FlatVector<T>>()->set(context.rowIdx, val);
      res = true;
    } else {
      if (selector_->shouldReadNode(context.colIdx)) {
        auto typeKind = selector_->getRequestType(context.colIdx)->kind();
        switch (typeKind) {
          case TypeKind::BOOLEAN: {
            saveScalarValue<T, bool>(context, val);
            break;
          }
          case TypeKind::INTEGER: {
            saveScalarValue<T, int32_t>(context, val);
            break;
          }
          case TypeKind::TINYINT: {
            saveScalarValue<T, int8_t>(context, val);
            break;
          }
          case TypeKind::SMALLINT: {
            saveScalarValue<T, int16_t>(context, val);
            break;
          }
          case TypeKind::BIGINT: {
            saveScalarValue<T, int64_t>(context, val);
            break;
          }
          case TypeKind::HUGEINT: {
            saveScalarValue<T, int128_t>(context, val);
            break;
          }
          case TypeKind::REAL: {
            saveScalarValue<T, float>(context, val);
            break;
          }
          case TypeKind::DOUBLE: {
            saveScalarValue<T, double>(context, val);
            break;
          }
          case TypeKind::VARCHAR:
          case TypeKind::VARBINARY: {
            saveScalarValue<T, StringView>(context, val);
            break;
          }
          case TypeKind::TIMESTAMP: {
            saveScalarValue<T, Timestamp>(context, val);
            break;
          }
          default: {
            VELOX_FAIL("not a scalar type!");
          }
        }
        res = true;
      }
    }
    return res;
  }

 private:
  const std::shared_ptr<ColumnSelector> selector_;
  const std::shared_ptr<const RowType> schema_;
};

} // namespace facebook::velox::hidi
