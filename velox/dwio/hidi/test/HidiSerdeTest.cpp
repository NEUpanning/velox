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

#include <gtest/gtest.h>
#include "velox/dwio/hidi/reader/HidiSerde.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::hidi;

namespace {
  auto defaultPool = memory::addDefaultLeafMemoryPool();
  const RowTypePtr primitiveType = ROW({
      {"c0", INTEGER()},
      {"c1", BIGINT()},
      {"c2", VARCHAR()},
      {"c3", BOOLEAN()},
      {"c4", TINYINT()},
      {"c5", SMALLINT()},
      {"c6", REAL()},
      {"c7", DOUBLE()},
      {"c8", VARBINARY()},
      {"c9", TIMESTAMP()},
      {"c10", INTEGER()} // date
    });
}

class HidiSerdeTest : public testing::Test {};

TEST_F(HidiSerdeTest, TestPrimitiveType) {
  HidiSerde serde(std::make_shared<ColumnSelector>(primitiveType), primitiveType);

  auto rowVector = BaseVector::create<RowVector>(primitiveType, 1, defaultPool.get());
  // generate by Hive's HidiSerde
  const char buff[] = {
      1, -114, 4, -46, 1, -115, 1, -30, 64, 1, 72, 73, 68, 73, 0,
      1, 2, 1, -28, 1, -125, -24, 1, -64, 72, -11, -61, 1, -64, 9,
      33, -5, 77, 18, -40, 74, 1, 72, 73, 68, 73, 0, 1, -128, 0, 0,
      102, 23, -69, 36, 34, 54, -117, -128, 1, -116, 102, 23, -58, 64
  };
  int32_t len = sizeof(buff) / sizeof(*buff);
  DeserializeContext context(buff, len);
  serde.deserializeRow<true>(context, rowVector->children());

  EXPECT_EQ(1234, rowVector->childAt(0)->as<FlatVector<int32_t>>()->valueAt(0));
  EXPECT_EQ(123456, rowVector->childAt(1)->as<FlatVector<int64_t>>()->valueAt(0));
  EXPECT_EQ("HIDI", rowVector->childAt(2)->as<FlatVector<StringView>>()->valueAt(0));
  EXPECT_TRUE(rowVector->childAt(3)->as<FlatVector<bool>>()->valueAt(0));
  EXPECT_EQ(100, rowVector->childAt(4)->as<FlatVector<int8_t>>()->valueAt(0));
  EXPECT_EQ(1000, rowVector->childAt(5)->as<FlatVector<int16_t>>()->valueAt(0));
  EXPECT_EQ(3.14f, rowVector->childAt(6)->as<FlatVector<float>>()->valueAt(0));
  EXPECT_EQ(3.1415926d, rowVector->childAt(7)->as<FlatVector<double>>()->valueAt(0));
  EXPECT_EQ("HIDI", rowVector->childAt(8)->as<FlatVector<StringView>>()->valueAt(0));
  auto timestamp = rowVector->childAt(9)->as<FlatVector<Timestamp>>()->valueAt(0);
  EXPECT_EQ(1712831268, timestamp.getSeconds());
  EXPECT_EQ(574000000, timestamp.getNanos());
  EXPECT_EQ(1712834112, rowVector->childAt(10)->as<FlatVector<int32_t>>()->valueAt(0));
}

TEST_F(HidiSerdeTest, TestNullValue) {
  HidiSerde serde(std::make_shared<ColumnSelector>(primitiveType), primitiveType);

  auto rowVector = BaseVector::create<RowVector>(primitiveType, 1, defaultPool.get());
  const char buff[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  int32_t len = sizeof(buff) / sizeof(*buff);
  DeserializeContext context(buff, len);
  serde.deserializeRow<true>(context, rowVector->children());

  EXPECT_TRUE(rowVector->childAt(0)->as<FlatVector<int32_t>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(1)->as<FlatVector<int64_t>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(2)->as<FlatVector<StringView>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(3)->as<FlatVector<bool>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(4)->as<FlatVector<int8_t>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(5)->as<FlatVector<int16_t>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(6)->as<FlatVector<float>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(7)->as<FlatVector<double>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(8)->as<FlatVector<StringView>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(9)->as<FlatVector<Timestamp>>()->isNullAt(0));
  EXPECT_TRUE(rowVector->childAt(10)->as<FlatVector<int32_t>>()->isNullAt(0));
}

TEST_F(HidiSerdeTest, TestDecimalType) {
  const RowTypePtr wrongType = ROW({{"c0", HUGEINT()}});
  HidiSerde serde(std::make_shared<ColumnSelector>(wrongType), wrongType);
  auto rowVector = BaseVector::create<RowVector>(wrongType, 1, defaultPool.get());
  // generate by Hive's HidiSerde, value is 0.0987654321
  const char buff[] = {1, 2, 127, -1, -1, -1, 57, 56, 55, 54, 53, 52, 51, 50, 49, 0};
  int32_t len = sizeof(buff) / sizeof(*buff);
  DeserializeContext context(buff, len);
  EXPECT_THROW(
      serde.deserializeRow<true>(context, rowVector->children()), VeloxException);

  // long decimal positive value
  const RowTypePtr type = ROW({{"c0", DECIMAL(20, 10)}});
  HidiSerde serde2(std::make_shared<ColumnSelector>(type), type);
  rowVector = BaseVector::create<RowVector>(type, 1, defaultPool.get());
  context.reset();
  serde2.deserializeRow<true>(context, rowVector->children());
  auto longVal = DecimalUtil::toString(
      rowVector->childAt(0)->as<FlatVector<int128_t>>()->valueAt(0), type->childAt(0));
  EXPECT_EQ("0.0987654321", longVal);

  // short decimal positive value
  const RowTypePtr type2 = ROW({{"c0", DECIMAL(11, 7)}});
  HidiSerde serde3(std::make_shared<ColumnSelector>(type2), type2);
  rowVector = BaseVector::create<RowVector>(type2, 1, defaultPool.get());
  context.reset();
  serde3.deserializeRow<true>(context, rowVector->children());
  auto decimalVal = DecimalUtil::toString(
      rowVector->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0), type2->childAt(0));
  EXPECT_EQ("0.0987654", decimalVal);

  // short decimal negative value -1234.123456789
  const char buff2[] = {
      1, 0, 127, -1, -1, -4, -50, -51, -52, -53, -50,
      -51, -52, -53, -54, -55, -56, -57, -58, -1
  };
  len = sizeof(buff2) / sizeof(*buff2);
  rowVector = BaseVector::create<RowVector>(type2, 1, defaultPool.get());
  DeserializeContext context2(buff2, len);
  serde3.deserializeRow<true>(context2, rowVector->children());
  decimalVal = DecimalUtil::toString(
      rowVector->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0), type2->childAt(0));
  EXPECT_EQ("-1234.1234567", decimalVal);
}

TEST_F(HidiSerdeTest, TestColumnSelector) {
  const RowTypePtr rowType = ROW({
      {"c0", ARRAY(VARCHAR())},
      {"c1", MAP(BIGINT(), REAL())}
  });
  auto rootTypeId = TypeWithId::create(rowType);
  EXPECT_EQ(0, rootTypeId->id());
  auto arrayTypeId = rootTypeId->childAt(0);
  EXPECT_EQ(1, arrayTypeId->id());
  auto eltTypeId = arrayTypeId->childAt(0);
  EXPECT_EQ(2, eltTypeId->id());
  auto mapTypeId = rootTypeId->childAt(1);
  EXPECT_EQ(3, mapTypeId->id());
  auto keyTypeId = mapTypeId->childAt(0);
  EXPECT_EQ(4, keyTypeId->id());
  auto valTypeId = mapTypeId->childAt(1);
  EXPECT_EQ(5, valTypeId->id());
  std::vector<uint64_t> nodes = {4, 5};
  auto selector = std::make_shared<ColumnSelector>(rowType, nodes, true);
  EXPECT_FALSE(selector->shouldReadAll());
  EXPECT_TRUE(selector->shouldReadNode(4));
  EXPECT_TRUE(selector->shouldReadNode(5));

  std::vector<std::string> columnNames;
  columnNames.push_back("c1");
  selector = std::make_shared<ColumnSelector>(rowType, columnNames);
  EXPECT_TRUE(selector->shouldReadNode(3));
  EXPECT_TRUE(selector->shouldReadNode(4));
  EXPECT_TRUE(selector->shouldReadNode(5));
}

TEST_F(HidiSerdeTest, TestArrayType) {
  const RowTypePtr rowType = ROW({
      {"c0", ARRAY(VARCHAR())}
  });
  HidiSerde serde(std::make_shared<ColumnSelector>(rowType), rowType);
  auto rowVector = BaseVector::create<RowVector>(rowType, 1, defaultPool.get());
  const char buff[] = {
      1, 1, 1, 79, 82, 67, 0, 1, 0, 1, 1, 80, 65, 82, 81, 85, 69, 84, 0, 0
  };
  int32_t len = sizeof(buff) / sizeof(*buff);
  DeserializeContext context(buff, len);
  serde.deserializeRow<true>(context, rowVector->children());
  auto arrayVec = rowVector->childAt(0)->as<ArrayVector>();
  EXPECT_EQ(1, arrayVec->size());
  EXPECT_EQ(context.bufSize, context.bufIdx);
  EXPECT_EQ(2, context.colIdx);
  EXPECT_EQ(0, context.rowIdx);
  auto elts = arrayVec->elements();
  EXPECT_EQ(0, arrayVec->offsetAt(0));
  EXPECT_EQ(3, arrayVec->sizeAt(0));
  EXPECT_EQ("ORC", elts->toString(0));
  EXPECT_TRUE(elts->isNullAt(1));
  EXPECT_EQ("PARQUET", elts->toString(2));
  // test do deserialize only
  context.reset();
  std::vector<uint64_t> nodes = {0};
  HidiSerde serde2(std::make_shared<ColumnSelector>(rowType, nodes, true), rowType);
  auto rowVector2 = BaseVector::create<RowVector>(rowType, 0, defaultPool.get());
  serde2.deserializeRow<false>(context, rowVector2->children());
  arrayVec = rowVector2->childAt(0)->as<ArrayVector>();
  EXPECT_EQ(0, arrayVec->size());
  EXPECT_EQ(context.bufSize, context.bufIdx);
  EXPECT_EQ(2, context.colIdx);
  EXPECT_EQ(0, context.rowIdx);
}

TEST_F(HidiSerdeTest, TestMapType) {
  const RowTypePtr rowType = ROW({
      {"c0", MAP(VARCHAR(), REAL())}
  });
  HidiSerde serde(std::make_shared<ColumnSelector>(rowType), rowType);
  auto rowVector = BaseVector::create<RowVector>(rowType, 1, defaultPool.get());
  const char buff[] = {
      1, 1, 1, 79, 82, 67, 0, 1, -64, 72, -11, -61, 1, 0, 1, -64,
      73, -103, -102, 1, 1, 80, 65, 82, 81, 85, 69, 84, 0, 0, 0
  };
  int32_t len = sizeof(buff) / sizeof(*buff);
  DeserializeContext context(buff, len);
  serde.deserializeRow<true>(context, rowVector->children());
  auto mapVec = rowVector->childAt(0)->as<MapVector>();
  auto keysVec = mapVec->mapKeys()->as<FlatVector<StringView>>();
  auto valsVec = mapVec->mapValues()->as<FlatVector<float>>();
  EXPECT_EQ(1, mapVec->size());
  EXPECT_EQ(context.bufSize, context.bufIdx);
  EXPECT_EQ(3, context.colIdx);
  EXPECT_EQ(0, context.rowIdx);
  EXPECT_EQ(0, mapVec->offsetAt(0));
  EXPECT_EQ(3, mapVec->sizeAt(0));
  EXPECT_EQ("ORC", keysVec->valueAt(0));
  EXPECT_EQ(3.14f, valsVec->valueAt(0));
  EXPECT_TRUE(keysVec->isNullAt(1));
  EXPECT_EQ(3.15f, valsVec->valueAt(1));
  EXPECT_EQ("PARQUET", keysVec->valueAt(2));
  EXPECT_TRUE(valsVec->isNullAt(2));
  // test do deserialize only
  context.reset();
  std::vector<uint64_t> nodes = {0};
  HidiSerde serde2(std::make_shared<ColumnSelector>(rowType, nodes, true), rowType);
  auto rowVector2 = BaseVector::create<RowVector>(rowType, 0, defaultPool.get());
  serde2.deserializeRow<false>(context, rowVector2->children());
  mapVec = rowVector2->childAt(0)->as<MapVector>();
  EXPECT_EQ(0, mapVec->size());
  EXPECT_EQ(3, context.colIdx);
  EXPECT_EQ(0, context.rowIdx);
  EXPECT_EQ(context.bufSize, context.bufIdx);
}

TEST_F(HidiSerdeTest, TestStructType) {
  const RowTypePtr rowType = ROW({
      {"c0", ROW({{"c0_0", VARCHAR()},{"c0_1", DOUBLE()}})}
  });
  HidiSerde serde(std::make_shared<ColumnSelector>(rowType), rowType);
  auto rowVector = BaseVector::create<RowVector>(rowType, 1, defaultPool.get());
  const char buff[] = {
      1, 1, 79, 82, 67, 0, 1, -64, 9, 33, -5, 77, 18, -40, 74
  };
  int32_t len = sizeof(buff) / sizeof(*buff);
  DeserializeContext context(buff, len);
  serde.deserializeRow<true>(context, rowVector->children());
  auto childVec = rowVector->childAt(0)->as<RowVector>();
  EXPECT_EQ(1, childVec->size());
  EXPECT_EQ(context.bufSize, context.bufIdx);
  EXPECT_EQ(3, context.colIdx);
  EXPECT_EQ(0, context.rowIdx);
  EXPECT_EQ("ORC", childVec->childAt(0)->as<FlatVector<StringView>>()->valueAt(0));
  EXPECT_EQ(3.1415926d, childVec->childAt(1)->as<FlatVector<double>>()->valueAt(0));
  // test do deserialize only
  context.reset();
  std::vector<uint64_t> nodes = {0};
  HidiSerde serde2(std::make_shared<ColumnSelector>(rowType, nodes, true), rowType);
  auto rowVector2 = BaseVector::create<RowVector>(rowType, 0, defaultPool.get());
  serde2.deserializeRow<false>(context, rowVector2->children());
  childVec = rowVector2->childAt(0)->as<RowVector>();
  EXPECT_EQ(0, childVec->size());
  EXPECT_EQ(3, context.colIdx);
  EXPECT_EQ(0, context.rowIdx);
  EXPECT_EQ(context.bufSize, context.bufIdx);
}
