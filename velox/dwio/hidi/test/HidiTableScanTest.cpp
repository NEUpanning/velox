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

#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/hidi/reader/HidiReader.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include <folly/init/Init.h>

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::common::test;
using namespace facebook::velox::exec::test;

class HidiTableScanTest : public HiveConnectorTestBase {
 protected:
  using OperatorTestBase::assertQuery;

  void SetUp() {
    hidi::registerHidiReaderFactory();
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, nullptr);
    connector::registerConnector(hiveConnector);
    functions::prestosql::registerAllScalarFunctions();
  }

  std::string getExampleFilePath(const std::string& fileName) const {
    std::string current_path = fs::current_path().c_str();
    std::string filePath = "velox/dwio/hidi/test/examples/" + fileName;
    return current_path + "/" + filePath;
  }

  void loadData(const std::string& filePath, const std::string& schema, bool compact) {
    rowType_ = asRowType(type::fbhive::HiveTypeParser().parse(schema));

    // get split info
    const size_t last_slash_idx = filePath.rfind('/');
    std::string dir = filePath.substr(0, last_slash_idx);
    std::string file = filePath.substr(last_slash_idx + 1);
    std::string splitInfo = "{\"dir\":\"file:" + dir + "\", \"files\":[\""+ file + "\"]}";
    auto hidiSplit = std::make_shared<HiveConnectorSplit>(
        kHiveConnectorId, splitInfo, dwio::common::FileFormat::HIDI);
    if (!compact) {
      hidiSplit->customSplitInfo.emplace("compactValues", "false");
    }
    splits_ = {hidiSplit};

    // get data by HidiReader
    dwio::common::ReaderOptions readerOpts(pool_.get());
    readerOpts.setFileFormat(FileFormat::HIDI);
    readerOpts.setFileSchema(rowType_);
    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(std::make_shared<ColumnSelector>(rowType_));
    std::vector<std::shared_ptr<ReadFile>> files {
      std::make_shared<LocalReadFile>(filePath)
    };
    hidi::HidiReader reader(files, readerOpts, rowReaderOpts, compact);
    VectorPtr batch = BaseVector::create(rowType_, 0, pool_.get());
    hidi::Scan scan;
    reader.seek(scan);
    reader.next(100, batch);
    std::vector<RowVectorPtr> vectors{
        std::dynamic_pointer_cast<RowVector>(batch)};
    createDuckDbTable(vectors);
  }

  void assertQuery(
      const std::string& filter,
      const std::string& sql,
      const std::vector<std::string>& outCols = {},
      const std::vector<std::string>& groupingKeys = {},
      const std::vector<std::string>& aggregates = {},
      const std::vector<std::string>& projects = {}) {
    RowTypePtr outputType = rowType_;
    if (!outCols.empty()) {
      std::vector<std::string> names;
      std::vector<TypePtr> types;
      for (auto& outCol : outCols) {
        names.emplace_back(outCol);
        types.emplace_back(rowType_->findChild(outCol));
      }
      outputType = ROW(std::move(names), std::move(types));
    }

    auto builder = PlanBuilder(pool_.get())
        .tableScan(
            "hive_table",
            outputType,
            {}/*columnAliases*/,
            {}/*subfieldFilters*/,
            filter/*remainingFilter*/,
            rowType_/*schema*/,
            false/*isFilterPushdownEnabled*/);
    if (!groupingKeys.empty()) {
      builder.singleAggregation(groupingKeys, aggregates);
    }
    if (!projects.empty()) {
      builder.project(projects);
    }
    auto plan = builder.planNode();
    assertQuery(plan, splits_, sql);
  }

 private:
  RowTypePtr rowType_;
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits_;
};

TEST_F(HidiTableScanTest, primitiveType) {
  std::string filePath = getExampleFilePath("primitive_mysqlv2");
  std::string schema = "struct<"
      "c0:INT,c1:BIGINT,c2:STRING,c3:BOOLEAN,c4:TINYINT,c5:SMALLINT,"
      "c6:FLOAT,c7:DOUBLE,c8:BINARY,c9:TIMESTAMP,c10:DATE,c11:DECIMAL(10,2)>";
  loadData(filePath, schema, true);
  assertQuery("", "SELECT * FROM tmp");
  assertQuery("c2 = 'a'", "SELECT c9,c2,c11 FROM tmp WHERE c2 = 'a'", {"c9","c2","c11"});
  assertQuery("c0 > 10", "SELECT c2,SUM(c1) FROM tmp WHERE c0 > 10 GROUP BY c2",
      {"c0", "c1", "c2"}, {"c2"}, {"sum(c1)"});
}

TEST_F(HidiTableScanTest, primitiveSplitValue) {
  std::string filePath = getExampleFilePath("primitive_split_value");
  std::string schema = "struct<"
      "c0:INT,c1:BIGINT,c2:STRING,c3:BOOLEAN,c4:TINYINT,c5:SMALLINT,"
      "c6:FLOAT,c7:DOUBLE,c8:BINARY,c9:TIMESTAMP,c10:DATE,c11:DECIMAL(10,2)>";
  loadData(filePath, schema, false);
  assertQuery("", "SELECT * FROM tmp");
  assertQuery("c2 = 'a'", "SELECT c9,c2,c11 FROM tmp WHERE c2 = 'a'", {"c9","c2","c11"});
  assertQuery("c0 > 10", "SELECT c2,SUM(c1) FROM tmp WHERE c0 > 10 GROUP BY c2",
      {"c0", "c1", "c2"}, {"c2"}, {"sum(c1)"});
}

TEST_F(HidiTableScanTest, splitValueError) {
  std::string filePath = getExampleFilePath("primitive_mysqlv2");
  std::string schema = "struct<"
      "c0:INT,c1:BIGINT,c2:STRING,c3:BOOLEAN,c4:TINYINT,c5:SMALLINT,"
      "c6:FLOAT,c7:DOUBLE,c8:BINARY,c9:TIMESTAMP,c10:DATE,c11:DECIMAL(10,2)>";
  EXPECT_THROW(loadData(filePath, schema, false), VeloxException);
}

TEST_F(HidiTableScanTest, complexType) {
  std::string filePath = getExampleFilePath("complex_mysqlv2");
  std::string schema = "struct<"
      "c0:STRING,"
      "c1:ARRAY<BIGINT>,"
      "c2:MAP<STRING,DOUBLE>,"
      "c3:STRUCT<c3_0:STRING,c3_1:INT>>";
  loadData(filePath, schema, true);
  assertQuery("", "SELECT * FROM tmp");
  assertQuery("c3.c3_0 = 'a'", "SELECT c1,c3.c3_1 FROM tmp WHERE c3.c3_0 = 'a'",
      {"c1","c3"}/*outCols*/, {}/*groupingKeys*/, {}/*aggregates*/, {"c1","c3.c3_1"});
  assertQuery("contains(c1, 14)", "SELECT c1 FROM tmp WHERE contains(c1, 14)", {"c1"});
  assertQuery("", "SELECT map_values(c2) FROM tmp",
      {"c2"}/*outCols*/, {}/*groupingKeys*/, {}/*aggregates*/, {"map_values(c2)"});
}

TEST_F(HidiTableScanTest, complexSplitValue) {
  std::string filePath = getExampleFilePath("complex_split_value");
  std::string schema = "struct<"
      "c0:STRING,"
      "c1:ARRAY<BIGINT>,"
      "c2:MAP<STRING,DOUBLE>,"
      "c3:STRUCT<c3_0:STRING,c3_1:INT>>";
  loadData(filePath, schema, false);
  assertQuery("", "SELECT * FROM tmp");
  assertQuery("c3.c3_0 = 'a'", "SELECT c1,c3.c3_1 FROM tmp WHERE c3.c3_0 = 'a'",
      {"c1","c3"}/*outCols*/, {}/*groupingKeys*/, {}/*aggregates*/, {"c1","c3.c3_1"});
  assertQuery("contains(c1, 14)", "SELECT c1 FROM tmp WHERE contains(c1, 14)", {"c1"});
  assertQuery("", "SELECT map_values(c2) FROM tmp",
      {"c2"}/*outCols*/, {}/*groupingKeys*/, {}/*aggregates*/, {"map_values(c2)"});
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
