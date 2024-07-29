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
#include "velox/common/base/Fs.h"
#include "velox/dwio/hidi/reader/HidiReader.h"
#include "velox/dwio/hidi/test/HidiTestBase.h"
#include "velox/type/fbhive/HiveTypeParser.h"

using namespace facebook::velox;
using namespace facebook::velox::hidi;

using HeapTestValue = uint64_t;
using Params = std::tuple<size_t, HeapTestValue, int64_t>;

const int64_t FLAGS_iters = 100000;

class HeapTest : public ::testing::TestWithParam<Params> {};
class HidiReaderTest : public HidiTestBase {
 public:
  void initCell(Cell& cell, const std::string& rowkey,
      int64_t timestamp, const std::string& tagStr = "") {
    cell.rowkey = &rowkey[0];
    cell.rowkeyLen = rowkey.size();
    cell.family = "f";
    cell.familyLen = 1;
    cell.column = "";
    cell.columnLen = 0;
    cell.type = 0;
    cell.timestamp = timestamp;
    cell.mvccLen = 0;
    cell.mvccType = MvccType::MYSQLV2;
    cell.tags = &tagStr[0];
    cell.tagsLen = tagStr.size();
  }

  std::string initTag(char val) {
    std::string res = "";
    // tag len
    res.push_back(0x00);
    res.push_back(0x06);
    // tag type
    res.push_back(HFileReader::KMvccTagType);
    // tag data
    res.push_back(val); // executeTime
    res.push_back(val); // sourceIpGlobalId
    res.push_back(val); // binlogFileIndex
    res.push_back(val); // binlogFileOffset
    res.push_back(val); // changeSetLineSeq
    return res;
  }

  std::string getExampleFilePath(const std::string& fileName) const {
    std::string current_path = fs::current_path().c_str();
    std::string filePath = "velox/dwio/hidi/test/examples/" + fileName;
    return current_path + "/" + filePath;
  }

  std::shared_ptr<const RowType> getSchemaType() {
    std::string schema = "struct<"
        "c0:INT,c1:BIGINT,c2:STRING,c3:BOOLEAN,c4:TINYINT,c5:SMALLINT,"
        "c6:FLOAT,c7:DOUBLE,c8:BINARY,c9:TIMESTAMP,c10:DATE,c11:DECIMAL(10,2)>";
    return asRowType(type::fbhive::HiveTypeParser().parse(schema));
  }
};

// Copy from RocksDB
TEST_P(HeapTest, Test) {
  // This test performs the same pseudorandom sequence of operations on a
  // BinaryHeap and an std::priority_queue, comparing output.  The three
  // possible operations are insert, replace top and pop.
  //
  // Insert is chosen slightly more often than the others so that the size of
  // the heap slowly grows.  Once the size heats the MAX_HEAP_SIZE limit, we
  // disallow inserting until the heap becomes empty, testing the "draining"
  // scenario.

  const auto MAX_HEAP_SIZE = std::get<0>(GetParam());
  const auto MAX_VALUE = std::get<1>(GetParam());
  const auto RNG_SEED = std::get<2>(GetParam());

  BinaryHeap<HeapTestValue> heap;
  std::priority_queue<HeapTestValue> ref;

  std::mt19937 rng(static_cast<unsigned int>(RNG_SEED));
  std::uniform_int_distribution<HeapTestValue> value_dist(0, MAX_VALUE);
  int ndrains = 0;
  bool draining = false;     // hit max size, draining until we empty the heap
  size_t size = 0;
  for (int64_t i = 0; i < FLAGS_iters; ++i) {
    if (size == 0) {
      draining = false;
    }

    if (!draining &&
        (size == 0 || std::bernoulli_distribution(0.4)(rng))) {
      // insert
      HeapTestValue val = value_dist(rng);
      heap.push(val);
      ref.push(val);
      ++size;
      if (size == MAX_HEAP_SIZE) {
        draining = true;
        ++ndrains;
      }
    } else if (std::bernoulli_distribution(0.5)(rng)) {
      // replace top
      HeapTestValue val = value_dist(rng);
      heap.replace_top(val);
      ref.pop();
      ref.push(val);
    } else {
      // pop
      assert(size > 0);
      heap.pop();
      ref.pop();
      --size;
    }

    // After every operation, check that the public methods give the same
    // results
    assert((size == 0) == ref.empty());
    ASSERT_EQ(size == 0, heap.empty());
    if (size > 0) {
      ASSERT_EQ(ref.top(), heap.top());
    }
  }

  // Probabilities should be set up to occasionally hit the max heap size and
  // drain it
  assert(ndrains > 0);

  heap.clear();
  ASSERT_TRUE(heap.empty());
}

INSTANTIATE_TEST_CASE_P(
  Basic, HeapTest,
  ::testing::Values(Params(1000, 3000, 0x1b575cf05b708945))
);
// Mid-size heap with small values (many duplicates)
INSTANTIATE_TEST_CASE_P(
  SmallValues, HeapTest,
  ::testing::Values(Params(100, 10, 0x5ae213f7bd5dccd0))
);
// Small heap, large value range (no duplicates)
INSTANTIATE_TEST_CASE_P(
  SmallHeap, HeapTest,
  ::testing::Values(Params(10, ULLONG_MAX, 0x3e1fa8f4d01707cf))
);
// Two-element heap
INSTANTIATE_TEST_CASE_P(
  TwoElementHeap, HeapTest,
  ::testing::Values(Params(2, 5, 0x4b5e13ea988c6abc))
);
// One-element heap
INSTANTIATE_TEST_CASE_P(
  OneElementHeap, HeapTest,
  ::testing::Values(Params(1, 3, 0x176a1019ab0b612e))
);

TEST_F(HidiReaderTest, TestKVHeap) {
  BinaryHeap<Cell, CellComparator> heap;
  Cell cell, cell2, cell3, cell4;

  // compare rowkey and timestamp
  std::vector<std::string> expectRowkeys{
      "4341670", "4341670", "4389645", "4455618"
  };
  std::vector<int64_t> expectTimestamps{4, 2, 1, 3};
  initCell(cell, expectRowkeys[2], expectTimestamps[2]);
  initCell(cell2, expectRowkeys[1], expectTimestamps[1]);
  initCell(cell3, expectRowkeys[3], expectTimestamps[3]);
  initCell(cell4, expectRowkeys[0], expectTimestamps[0]);
  heap.push(cell);
  heap.push(cell2);
  heap.push(cell3);
  heap.push(cell4);
  int index = 0;
  while (!heap.empty()) {
    auto& kv = heap.top();
    ASSERT_EQ(std::string(kv.rowkey), expectRowkeys[index++]);
    heap.pop();
  }
  ASSERT_TRUE(heap.empty());

  // compare custom mvcc
  std::vector<char> expects{103, 102, 101, 100};
  std::string tagStr = initTag(expects[3]);
  std::string tagStr2 = initTag(expects[1]);
  std::string tagStr3 = initTag(expects[0]);
  std::string tagStr4 = initTag(expects[2]);
  initCell(cell, expectRowkeys[0], 0, tagStr);
  initCell(cell2, expectRowkeys[0], 0, tagStr2);
  initCell(cell3, expectRowkeys[0], 0, tagStr3);
  initCell(cell4, expectRowkeys[0], 0, tagStr4);
  heap.push(cell);
  heap.push(cell2);
  heap.push(cell3);
  heap.push(cell4);
  index = 0;
  while (!heap.empty()) {
    auto& kv = heap.top();
    MysqlV2MvccInfo mvccInfo;
    ASSERT_TRUE(kv.getMysqlV2MvccInfo(mvccInfo));
    ASSERT_EQ(mvccInfo.executeTime, (int64_t) expects[index++]);
    heap.pop();
  }
}

TEST_F(HidiReaderTest, TestDeduplication) {
  std::vector<std::shared_ptr<ReadFile>> files {
      std::make_shared<LocalReadFile>(getExampleFilePath("primitive_seqid_base")),
      std::make_shared<LocalReadFile>(getExampleFilePath("primitive_mysqlv2"))
  };
  auto type = getSchemaType();
  dwio::common::ReaderOptions readerOpts(defaultPool.get());
  readerOpts.setFileFormat(FileFormat::HIDI);
  readerOpts.setFileSchema(type);
  dwio::common::RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(std::make_shared<ColumnSelector>(type));

  HidiReader reader(files, readerOpts, rowReaderOpts);
  auto vector = BaseVector::create(type, 0, defaultPool.get());
  Scan scan;
  EXPECT_TRUE(reader.seek(scan));
  int entryCount = 0;
  int batchCount = 100;
  int readCount = batchCount;
  while (readCount == batchCount) {
    readCount = reader.next(batchCount, vector, nullptr);
    entryCount += readCount;
  }
  ASSERT_EQ(entryCount, 52);
  ASSERT_EQ(vector->size(), 52);

  auto rowVector = vector->as<RowVector>();
  EXPECT_EQ(39, rowVector->childAt(0)->as<FlatVector<int32_t>>()->valueAt(0));
  EXPECT_EQ(39, rowVector->childAt(1)->as<FlatVector<int64_t>>()->valueAt(0));
  EXPECT_EQ("m", rowVector->childAt(2)->as<FlatVector<StringView>>()->valueAt(0));
  EXPECT_TRUE(rowVector->childAt(3)->as<FlatVector<bool>>()->valueAt(0));
  EXPECT_EQ(39, rowVector->childAt(4)->as<FlatVector<int8_t>>()->valueAt(0));
  EXPECT_EQ(39, rowVector->childAt(5)->as<FlatVector<int16_t>>()->valueAt(0));
  EXPECT_EQ(39.0, rowVector->childAt(6)->as<FlatVector<float>>()->valueAt(0));
  EXPECT_EQ(39.0d, rowVector->childAt(7)->as<FlatVector<double>>()->valueAt(0));
  EXPECT_EQ("m", rowVector->childAt(8)->as<FlatVector<StringView>>()->valueAt(0));
  auto timestamp = rowVector->childAt(9)->as<FlatVector<Timestamp>>()->valueAt(0);
  EXPECT_EQ(1715071020, timestamp.getSeconds());
  EXPECT_EQ(0, timestamp.getNanos());
  EXPECT_EQ(19850, rowVector->childAt(10)->as<FlatVector<int32_t>>()->valueAt(0));
  auto decimalVal = DecimalUtil::toString(
      rowVector->childAt(11)->as<FlatVector<int64_t>>()->valueAt(0), type->childAt(11));
  EXPECT_EQ("-1234567.89", decimalVal);
}

TEST_F(HidiReaderTest, TestBatch) {
  std::vector<std::shared_ptr<ReadFile>> files {
      std::make_shared<LocalReadFile>(getExampleFilePath("primitive_mysqlv2"))
  };
  auto type = getSchemaType();
  std::string schema = "struct<c0:INT,c2:STRING>";
  auto outputType =  asRowType(type::fbhive::HiveTypeParser().parse(schema));
  dwio::common::ReaderOptions readerOpts(defaultPool.get());
  readerOpts.setFileFormat(FileFormat::HIDI);
  readerOpts.setFileSchema(type);
  dwio::common::RowReaderOptions rowReaderOpts;
  std::vector<uint64_t> nodes = {1, 3};
  rowReaderOpts.select(std::make_shared<ColumnSelector>(type, nodes, true));

  HidiReader reader(files, readerOpts, rowReaderOpts);
  auto vector = BaseVector::create(outputType, 0, defaultPool.get());
  Scan scan;
  EXPECT_TRUE(reader.seek(scan));
  int entryCount = 0;
  int batchCount = 10;
  int batchs = 0;
  int readCount = batchCount;
  while (readCount == batchCount) {
    readCount = reader.next(batchCount, vector, nullptr);
    entryCount += readCount;
    batchs++;
  }
  ASSERT_EQ(entryCount, 52);
  ASSERT_EQ(batchs, 6);
  ASSERT_EQ(vector->size(), 2); // last batch
  ASSERT_EQ(vector->as<RowVector>()->childAt(0)->size(), 2);
  ASSERT_EQ(vector->as<RowVector>()->childAt(1)->size(), 2);
}

TEST_F(HidiReaderTest, TestNoData) {
  std::vector<std::shared_ptr<ReadFile>> files {
      std::make_shared<LocalReadFile>(getExampleFilePath("primitive_mysqlv2"))
  };
  auto type = getSchemaType();
  dwio::common::ReaderOptions readerOpts(defaultPool.get());
  readerOpts.setFileFormat(FileFormat::HIDI);
  readerOpts.setFileSchema(type);
  dwio::common::RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(std::make_shared<ColumnSelector>(type));
  HidiReader reader(files, readerOpts, rowReaderOpts);
  auto vector = BaseVector::create(type, 0, defaultPool.get());
  Scan scan;
  scan.endKey = "03681";
  EXPECT_FALSE(reader.seek(scan));
  ASSERT_EQ(reader.next(100, vector, nullptr), 0);
}
