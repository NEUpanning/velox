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

#include "velox/connectors/hive/HivePartitionUtil.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/catalog/fbhive/FileUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "gtest/gtest.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::dwio::catalog::fbhive;

class HivePartitionUtilTest : public ::testing::Test,
                              public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  template <typename T>
  VectorPtr makeDictionary(const std::vector<T>& data) {
    auto base = makeFlatVector(data);
    auto indices =
        makeIndices(data.size() * 10, [](auto row) { return row / 10; });
    return wrapInDictionary(indices, data.size(), base);
  };

  RowVectorPtr makePartitionsVector(
      RowVectorPtr input,
      const std::vector<column_index_t>& partitionChannels) {
    std::vector<VectorPtr> partitions;
    std::vector<std::string> partitonKeyNames;
    std::vector<TypePtr> partitionKeyTypes;

    RowTypePtr inputType = asRowType(input->type());
    for (column_index_t channel : partitionChannels) {
      partitions.push_back(input->childAt(channel));
      partitonKeyNames.push_back(inputType->nameOf(channel));
      partitionKeyTypes.push_back(inputType->childAt(channel));
    }

    return std::make_shared<RowVector>(
        pool(),
        ROW(std::move(partitonKeyNames), std::move(partitionKeyTypes)),
        nullptr,
        input->size(),
        partitions);
  }
};

TEST_F(HivePartitionUtilTest, partitionName) {
  {
    RowVectorPtr input = makeRowVector(
        {"flat_bool_col",
         "flat_tinyint_col",
         "flat_smallint_col",
         "flat_int_col",
         "flat_bigint_col",
         "dict_string_col",
         "const_date_col",
         "flat_timestamp_col"},
        {makeFlatVector<bool>(std::vector<bool>{false}),
         makeFlatVector<int8_t>(std::vector<int8_t>{10}),
         makeFlatVector<int16_t>(std::vector<int16_t>{100}),
         makeFlatVector<int32_t>(std::vector<int32_t>{1000}),
         makeFlatVector<int64_t>(std::vector<int64_t>{10000}),
         makeDictionary<StringView>(std::vector<StringView>{"str1000"}),
         makeConstant<int32_t>(10000, 1, DATE()),
         makeFlatVector<Timestamp>(
             std::vector<Timestamp>{Timestamp::fromMillis(1577836800000)})});

    std::vector<std::string> expectedPartitionKeyValues{
        "flat_bool_col=false",
        "flat_tinyint_col=10",
        "flat_smallint_col=100",
        "flat_int_col=1000",
        "flat_bigint_col=10000",
        "dict_string_col=str1000",
        "const_date_col=1997-05-19",
        "flat_timestamp_col=2019-12-31 16%3A00%3A00.0"};

    std::vector<column_index_t> partitionChannels;
    for (auto i = 1; i <= expectedPartitionKeyValues.size(); i++) {
      partitionChannels.resize(i);
      std::iota(partitionChannels.begin(), partitionChannels.end(), 0);

      EXPECT_EQ(
          FileUtils::makePartName(
              extractPartitionKeyValues(
                  makePartitionsVector(input, partitionChannels), 0),
              true),
          folly::join(
              "/",
              std::vector<std::string>(
                  expectedPartitionKeyValues.data(),
                  expectedPartitionKeyValues.data() + i)));
    }
  }

  // Test unsupported partition type.
  {
    RowVectorPtr input = makeRowVector(
        {"map_col"},
        {makeMapVector<int32_t, StringView>(
            {{{1, "str1000"}, {2, "str2000"}}})});

    std::vector<column_index_t> partitionChannels{0};

    VELOX_ASSERT_THROW(
        FileUtils::makePartName(
            extractPartitionKeyValues(
                makePartitionsVector(input, partitionChannels), 0),
            true),
        "Unsupported partition type: MAP");
  }
}

TEST_F(HivePartitionUtilTest, partitionNameForNull) {
  std::vector<std::string> partitionColumnNames{
      "flat_bool_col",
      "flat_tinyint_col",
      "flat_smallint_col",
      "flat_int_col",
      "flat_bigint_col",
      "flat_string_col",
      "const_date_col",
      "flat_timestamp_col"};

  RowVectorPtr input = makeRowVector(
      partitionColumnNames,
      {makeNullableFlatVector<bool>({std::nullopt}),
       makeNullableFlatVector<int8_t>({std::nullopt}),
       makeNullableFlatVector<int16_t>({std::nullopt}),
       makeNullableFlatVector<int32_t>({std::nullopt}),
       makeNullableFlatVector<int64_t>({std::nullopt}),
       makeNullableFlatVector<StringView>({std::nullopt}),
       makeConstant<int32_t>(std::nullopt, 1, DATE()),
       makeNullableFlatVector<Timestamp>({std::nullopt})});

  for (auto i = 0; i < partitionColumnNames.size(); i++) {
    std::vector<column_index_t> partitionChannels = {(column_index_t)i};
    auto partitionEntries = extractPartitionKeyValues(
        makePartitionsVector(input, partitionChannels), 0);
    EXPECT_EQ(1, partitionEntries.size());
    EXPECT_EQ(partitionColumnNames[i], partitionEntries[0].first);
    EXPECT_EQ("", partitionEntries[0].second);
  }
}

TEST_F(HivePartitionUtilTest, timestampPartitionValueFormatting) {
  // Test timestamp partition value formatting to match Presto's
  // java.sql.Timestamp.toString() behavior: removes trailing zeros but keeps at
  // least one decimal place
  std::vector<Timestamp> timestamps = {
      // Test case 1: All zeros in fractional seconds -> should become ".0"
      Timestamp(
          0, 0), // 1970-01-01 00:00:00.000 UTC -> 1969-12-31 16:00:00.0 PST

      // Test case 2: Trailing zeros should be removed
      Timestamp(0, 980000000), // 1970-01-01 00:00:00.980 UTC -> 1969-12-31
      // 16:00:00.98 PST

      // Test case 3: No trailing zeros, should remain unchanged
      Timestamp(0, 123000000), // 1970-01-01 00:00:00.123 UTC -> 1969-12-31
      // 16:00:00.123 PST
  };

  // Expected values account for timezone conversion to PST (UTC-8)
  std::vector<std::string> expectedValues = {
      "1969-12-31 16:00:00.0", // .000 -> .0 (converted to PST)
      "1969-12-31 16:00:00.98", // .980 -> .98 (converted to PST)
      "1969-12-31 16:00:00.123", // .123 -> .123 (converted to PST)
  };

  RowVectorPtr input =
      makeRowVector({"timestamp_col"}, {makeFlatVector<Timestamp>(timestamps)});

  std::vector<column_index_t> partitionChannels{0};
  auto partitionsVector = makePartitionsVector(input, partitionChannels);

  for (size_t i = 0; i < timestamps.size(); i++) {
    auto partitionEntries = extractPartitionKeyValues(
        partitionsVector, static_cast<vector_size_t>(i));

    EXPECT_EQ(1, partitionEntries.size());
    EXPECT_EQ("timestamp_col", partitionEntries[0].first);
    EXPECT_EQ(expectedValues[i], partitionEntries[0].second)
        << "Failed for timestamp index " << i << " with value "
        << timestamps[i].toString();
  }
}
