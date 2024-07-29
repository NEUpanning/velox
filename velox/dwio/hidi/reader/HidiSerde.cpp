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
#include "velox/dwio/common/TypeUtils.h"
#include "velox/dwio/hidi/reader/HidiSerde.h"
#include "velox/dwio/hidi/reader/BytesUtils.h"

namespace {

constexpr long SEVEN_BYTE_LONG_SIGN_FLIP = 0xff80L << 48;

union int_to_float_convertor {
  int32_t int_bits;
  float float_bits;
};

union long_to_double_convertor {
  int64_t long_bits;
  double double_bits;
};

// Corresponding to Hive's TimestampWritable::readSevenByteLong
inline int64_t readSevenByteLong(const char* bytes, int offset) {
  return (((0xFFL & bytes[offset]) << 56)
      | ((0xFFL & bytes[offset+1]) << 48)
      | ((0xFFL & bytes[offset+2]) << 40)
      | ((0xFFL & bytes[offset+3]) << 32)
      | ((0xFFL & bytes[offset+4]) << 24)
      | ((0xFFL & bytes[offset+5]) << 16)
      | ((0xFFL & bytes[offset+6]) << 8)) >> 8;
}

// Corresponding to Hive's TimestampWritable::bytesToInt
inline int bytesToInt(const char* bytes, int offset) {
  return ((0xFF & bytes[offset]) << 24)
      | ((0xFF & bytes[offset+1]) << 16)
      | ((0xFF & bytes[offset+2]) << 8)
      | (0xFF & bytes[offset+3]);
}

} // namespace

namespace facebook::velox::hidi {
HidiSerde::HidiSerde(
    const std::shared_ptr<ColumnSelector> selector,
    const std::shared_ptr<const RowType> schema)
    : selector_(selector), schema_(schema) {
  if (selector_ && !selector_->shouldReadAll()) {
    typeutils::checkTypeCompatibility(*schema_, *selector_);
  }
}

template <typename T>
T HidiSerde::deserializeDecimal(
    DeserializeContext& context, int32_t targetScale) const {
  char b = context.buff[context.bufIdx++] - 1;
  bool positive = b != -1;
  int32_t factor = context.buff[context.bufIdx++] ^ 0x80;
  for (int i = 0; i < 3; i++) {
    factor = (factor << 8) + (context.buff[context.bufIdx++] & 0xff);
  }
  if (!positive) {
    factor = -factor;
  }

  int length = 0;
  int idx = context.bufIdx;
  char c = positive ? context.buff[idx++] : context.buff[idx++] ^ 0xff;
  while (c != 0) {
    length++;
    c = positive ? context.buff[idx++] : context.buff[idx++] ^ 0xff;
  }

  std::string digits;
  for (int i = 0; i < length; ++i) {
    if (positive) {
      digits += context.buff[context.bufIdx++];
    } else {
      digits += context.buff[context.bufIdx++] ^ 0xff;
    }
  }
  context.bufIdx += (length + 1/*skip '\0'*/);

  T val;
  int32_t currentScale = std::abs(factor - (int32_t)length);

  if constexpr (std::is_same_v<T, std::int64_t>) { // Short Decimal
    val = std::stol(digits);
    if (targetScale > currentScale &&
        targetScale - currentScale <= ShortDecimalType::kMaxPrecision) {
      val *= static_cast<T>(DecimalUtil::kPowersOfTen[targetScale - currentScale]);
    } else if (targetScale < currentScale &&
        currentScale - targetScale <= ShortDecimalType::kMaxPrecision) {
      val /= static_cast<T>(DecimalUtil::kPowersOfTen[currentScale - targetScale]);
    } else if (targetScale != currentScale) {
      VELOX_FAIL("Decimal scale out of range");
    }
  } else { // Long Decimal
    val = std::stoll(digits);
    if (targetScale > currentScale) {
      while (targetScale > currentScale) {
        int32_t scaleAdjust = std::min<int32_t>(
            ShortDecimalType::kMaxPrecision, targetScale - currentScale);
        val *= DecimalUtil::kPowersOfTen[scaleAdjust];
        currentScale += scaleAdjust;
      }
    } else if (targetScale < currentScale) {
      while (currentScale > targetScale) {
        int32_t scaleAdjust = std::min<int32_t>(
            ShortDecimalType::kMaxPrecision, currentScale - targetScale);
        val /= DecimalUtil::kPowersOfTen[scaleAdjust];
        currentScale -= scaleAdjust;
      }
    }
  }
  if (!positive) {
    val = -val;
  }
  return val;
}

template <bool kReadAll>
bool HidiSerde::deserializeArray(DeserializeContext& context) const {
  bool res = false;
  context.colType = const_cast<Type*>(context.colType->childAt(0).get());
  if (kReadAll || selector_->shouldReadNode(context.colIdx)) {
    int32_t arrayIdx = context.rowIdx;
    auto resultArray = context.outVec->as<ArrayVector>();
    DWIO_ENSURE(resultArray, "Column should be Array Type.");
    context.outVec = resultArray->elements().get();
    int32_t offset = 0;
    if (arrayIdx > 0) {
      int32_t preIdx = arrayIdx - 1;
      offset = resultArray->offsetAt(preIdx) + resultArray->sizeAt(preIdx);
    } else {
      // do init when first value add
      context.outVec->resize(kArrayInitSize);
    }
    context.rowIdx = offset;
    while (context.hasRemaining() && context.buff[context.bufIdx++] != 0) {
      if (context.rowIdx >= context.outVec->size()) {
        // elements vector write full, double it
        context.outVec->resize(context.outVec->size() * 2);
      }
      if (context.buff[context.bufIdx++] != 0) {
        deserializeColumn<true>(context);
      } else {
        context.outVec->setNull(context.rowIdx, true);
      }
      context.rowIdx++;
    }
    // add entry to ArrayVector
    int32_t size = context.rowIdx - offset;
    resultArray->setOffsetAndSize(arrayIdx, offset, size);
    // restore original rowIdx
    context.rowIdx = arrayIdx;
    res = true;
  } else {
    // no need to set outVec and rowIdx, since value don't save
    while (context.hasRemaining() && context.buff[context.bufIdx++] != 0) {
      if (context.buff[context.bufIdx++] != 0) {
        deserializeColumn<false>(context);
      }
    }
  }
  context.colIdx++; // elements using independent colIdx
  return res;
}

template <bool kReadAll>
bool HidiSerde::deserializeMap(DeserializeContext& context) const {
  bool res = false;
  auto keyType = const_cast<Type*>(context.colType->childAt(0).get());
  auto valType = const_cast<Type*>(context.colType->childAt(1).get());
  uint32_t keyColIdx = context.colIdx + 1;
  uint32_t valColIdx = context.colIdx + 2;
  if (kReadAll || selector_->shouldReadNode(context.colIdx)) {
    auto resultMap = context.outVec->as<MapVector>();
    DWIO_ENSURE(resultMap, "Column should be Map Type.");
    auto resultKeys = resultMap->mapKeys().get();
    auto resultVals = resultMap->mapValues().get();
    int32_t offset = 0;
    int32_t mapIdx = context.rowIdx;
    if (mapIdx > 0) {
      int32_t preIdx = mapIdx - 1;
      offset = resultMap->offsetAt(preIdx) + resultMap->sizeAt(preIdx);
    } else {
      resultKeys->resize(kArrayInitSize);
      resultVals->resize(kArrayInitSize);
    }
    context.rowIdx = offset;
    while (context.hasRemaining() && context.buff[context.bufIdx++] != 0) {
      if (context.rowIdx >= resultKeys->size()) {
        // vector write full, double the capacity
        resultKeys->resize(resultKeys->size() * 2);
        resultVals->resize(resultVals->size() * 2);
      }
      // deserialize key
      context.colIdx = keyColIdx;
      context.outVec = resultKeys;
      context.colType = keyType;
      if (context.buff[context.bufIdx++] != 0) {
        deserializeColumn<true>(context);
      } else {
        context.outVec->setNull(context.rowIdx, true);
      }
      // deserialize value
      context.colIdx = valColIdx;
      context.outVec = resultVals;
      context.colType = valType;
      if (context.buff[context.bufIdx++] != 0) {
        deserializeColumn<true>(context);
      } else {
        context.outVec->setNull(context.rowIdx, true);
      }
      context.rowIdx++;
    }
    // add entry to MapVector
    int32_t size = context.rowIdx - offset;
    resultMap->setOffsetAndSize(mapIdx, offset, size);
    // restore original rowIdx
    context.rowIdx = mapIdx;
    res = true;
  } else {
    // no need to save, do deserialize only
    while (context.hasRemaining() && context.buff[context.bufIdx++] != 0) {
      context.colType = keyType;
      if (context.buff[context.bufIdx++] != 0) {
        deserializeColumn<false>(context);
      }
      context.colType = valType;
      if (context.buff[context.bufIdx++] != 0) {
        deserializeColumn<false>(context);
      }
    }
    context.colIdx = valColIdx;
  }
  return res;
}

template <bool kReadAll>
bool HidiSerde::deserializeColumn(DeserializeContext& context) const {
  switch (context.colType->kind()) {
    case TypeKind::BOOLEAN: {
      char b = context.buff[context.bufIdx++];
      DWIO_ENSURE(b == 1 || b == 2, "Boolean type has wrong value.");
      bool val = (b == 2);
      return saveToVector<kReadAll, bool>(context, val);
    }
    case TypeKind::TINYINT: {
      int8_t val = context.buff[context.bufIdx++] ^ 0x80;
      return saveToVector<kReadAll, int8_t>(context, val);
    }
    case TypeKind::SMALLINT: {
      int16_t val = context.buff[context.bufIdx++] ^ 0x80;
      val = (val << 8) + (context.buff[context.bufIdx++] & 0xff);
      return saveToVector<kReadAll, int16_t>(context, val);
    }
    case TypeKind::INTEGER: {
      int32_t val = (int32_t)readVLong(context.buff, context.bufIdx);
      return saveToVector<kReadAll, int32_t>(context, val);
    }
    case TypeKind::BIGINT: {
      int64_t val;
      if (context.colType->isDecimal()) {
        int32_t targetScale = context.colType->asShortDecimal().scale();
        if constexpr (!kReadAll) {
          if (selector_->shouldReadNode(context.colIdx)) {
            targetScale = selector_->getRequestType(context.colIdx)
                ->asShortDecimal().scale();
          }
        }
        val = deserializeDecimal<int64_t>(context, targetScale);
      } else {
        val = readVLong(context.buff, context.bufIdx);
      }
      return saveToVector<kReadAll, int64_t>(context, val);
    }
    case TypeKind::HUGEINT: {
      if (!context.colType->isDecimal()) {
        DWIO_RAISE("HUGEINT is not Long Decimal.");
      }
      int32_t targetScale = context.colType->asLongDecimal().scale();
      if constexpr (!kReadAll) {
        if (selector_->shouldReadNode(context.colIdx)) {
          targetScale = selector_->getRequestType(context.colIdx)
              ->asLongDecimal().scale();
        }
      }
      int128_t val = deserializeDecimal<int128_t>(context, targetScale);
      return saveToVector<kReadAll, int128_t>(context, val);
    }
    case TypeKind::REAL: {
      int_to_float_convertor convertor;
      for (int i = 0; i < 4; i++) {
        convertor.int_bits = (convertor.int_bits << 8)
            + (context.buff[context.bufIdx++] & 0xff);
      }
      if ((convertor.int_bits & (1 << 31)) == 0) {
        // negative number, flip all bits
        convertor.int_bits = ~convertor.int_bits;
      } else {
        // positive number, flip the first bit
        convertor.int_bits = convertor.int_bits ^ (1 << 31);
      }
      float val = convertor.float_bits;
      return saveToVector<kReadAll, float>(context, val);
    }
    case TypeKind::DOUBLE: {
      long_to_double_convertor convertor;
      for (int i = 0; i < 8; i++) {
        convertor.long_bits = (convertor.long_bits << 8)
            + (context.buff[context.bufIdx++] & 0xff);
      }
      if ((convertor.long_bits & (1L << 63)) == 0) {
        // negative number, flip all bits
        convertor.long_bits = ~convertor.long_bits;
      } else {
        // positive number, flip the first bit
        convertor.long_bits = convertor.long_bits ^ (1L << 63);
      }
      double val = convertor.double_bits;
      return saveToVector<kReadAll, double>(context, val);
    }
    // TODO chenxu14 test escape char
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      const char* colBuff = context.buff + context.bufIdx;
      auto len = strlen(colBuff);
      StringView val(colBuff, len);
      context.bufIdx += (len + 1/*skip '\0'*/);
      return saveToVector<kReadAll, StringView>(context, val);
    }
    // Corresponding to TimestampWritable::setBinarySortable
    case TypeKind::TIMESTAMP: {
      int64_t seconds = readSevenByteLong(context.buff, context.bufIdx)
          ^ SEVEN_BYTE_LONG_SIGN_FLIP;
      context.bufIdx += 7;
      uint64_t nanos = bytesToInt(context.buff, context.bufIdx);
      context.bufIdx += 4;
      Timestamp val(seconds, nanos);
      return saveToVector<kReadAll, Timestamp>(context, val);
    }
    case TypeKind::ARRAY: {
      return deserializeArray<kReadAll>(context);
    }
    case TypeKind::MAP: {
      return deserializeMap<kReadAll>(context);
    }
    case TypeKind::ROW: {
      bool res = false;
      if (kReadAll || selector_->shouldReadNode(context.colIdx)) {
        auto rowVector = context.outVec->as<RowVector>();
        DWIO_ENSURE(rowVector, "Column should be RowType.");
        deserializeRow<true>(context, rowVector->children(), false);
        res = true;
      } else {
        // no need to save, do deserialize only
        auto childSize = context.colType->size();
        uint32_t childIdx = 0;
        while (context.hasRemaining() && childIdx < childSize) {
          context.colIdx++;
          context.colType = const_cast<Type*>(
              selector_->getDataType(context.colIdx).get());
          if (context.buff[context.bufIdx++] != 0) {
            deserializeColumn<false>(context);
          }
          childIdx++;
        }
      }
      return res;
    }
    default:{
      DWIO_RAISE("Unsupported column type ", context.colType->kind());
      break;
    }
    return false;
  }
}

template <bool kReadAll>
void HidiSerde::deserializeRow(
    DeserializeContext& context,
    const std::vector<VectorPtr>& outputVecs,
    bool earlyResponse) const {
  auto outputSize = outputVecs.size();
  auto childSize = selector_->getDataType(context.colIdx)->size();
  uint32_t outIdx = 0;
  uint32_t childIdx = 0;
  while (context.hasRemaining() && childIdx < childSize) {
    context.colIdx++;
    context.colType = const_cast<Type*>(selector_->getDataType(context.colIdx).get());
    context.outVec = outputVecs[outIdx].get();
    // first bit indicates whether column has value
    if (context.buff[context.bufIdx++] != 0) {
      if (deserializeColumn<kReadAll>(context) // value saved to vector
          && (++outIdx == outputSize) && earlyResponse) {
        return;
      }
    } else { // column don't has value
      if constexpr (kReadAll) {
        context.outVec->setNull(context.rowIdx, true);
        if ((++outIdx == outputSize) && earlyResponse) {
          return;
        }
      } else {
        if (selector_->shouldReadNode(context.colIdx)) {
          context.outVec->setNull(context.rowIdx, true);
          if ((++outIdx == outputSize) && earlyResponse) {
            return;
          }
        }
      }
    }
    childIdx++;
  }
}

template void HidiSerde::deserializeRow<true>(
    DeserializeContext& context,
    const std::vector<VectorPtr>& outputVecs,
    bool earlyResponse) const;

template void HidiSerde::deserializeRow<false>(
    DeserializeContext& context,
    const std::vector<VectorPtr>& outputVecs,
    bool earlyResponse) const;

template bool HidiSerde::deserializeColumn<true>(
    DeserializeContext& context) const;

template bool HidiSerde::deserializeColumn<false>(
    DeserializeContext& context) const;

} // namespace facebook::velox::hidi
