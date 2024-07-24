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
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/hidi/reader/HidiReader.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/hidi/reader/HidiReader.h"
#include "velox/type/Type.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/BaseVector.h"
#include <folly/init/Init.h>
#include <algorithm>

using namespace facebook::velox;
using namespace facebook::velox::type;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::hidi;
using exec::test::HiveConnectorTestBase;

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  auto pool = memory::addDefaultLeafMemoryPool();
  std::string schema = "struct<c0:STRING,c1:ARRAY<BIGINT>,c2:MAP<STRING,DOUBLE>,c3:STRUCT<c3_0:STRING,c3_1:INT>>";
  auto rowType = asRowType(fbhive::HiveTypeParser().parse(schema));
  auto outputType = asRowType(fbhive::HiveTypeParser().parse("struct<c1:ARRAY<BIGINT>,c3:STRUCT<c3_0:STRING,c3_1:INT>>"));

  const std::string kHiveConnectorId = "test-hive";
  auto properties = std::make_shared<const core::MemConfig>();
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, properties);
  connector::registerConnector(hiveConnector);

  filesystems::registerLocalFileSystem();
  hidi::registerHidiReaderFactory();
  parse::registerTypeResolver();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  core::PlanNodeId scanNodeId;
  // SELECT c1,c3.c3_1 FROM tmp WHERE c3.c3_0 > 'x'
  auto readPlanFragment = exec::test::PlanBuilder()
      .tableScan(
          "hive_table",
          outputType,
          {}/*columnAliases*/,
          {}/*subfieldFilters*/,
          "c3.c3_0 > 'x'"/*remainingFilter*/,
          rowType/*schema*/,
          false/*isFilterPushdownEnabled*/)
      .capturePlanNodeId(scanNodeId)
   // .singleAggregation({"c2"}/*groupingKeys*/, {"sum(c1)"}/*aggregates*/)
      .project({"c1", "c3.c3_1"})
      .planFragment();
  printf("[plan]\n%s\n", readPlanFragment.planNode->toString(true, true).c_str());

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      std::make_shared<core::QueryCtx>(executor.get()));

  // prepare Split
  std::string splitInfo = R"({"dir":"file:/velox", "files":["complex_mysqlv2"]})";
  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId, splitInfo, dwio::common::FileFormat::HIDI);
  readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  readTask->noMoreSplits(scanNodeId);

  // get results
  RowVectorPtr rows = readTask->next();
  if (rows) {
    std::cout << "count : " << rows->size() << "\n";
    exec::test::DuckDbQueryRunner duckDb;
    std::vector<RowVectorPtr> data{rows};
    duckDb.createTable("test", data);
    exec::test::DuckDBQueryResult queryRes = duckDb.execute("select * from test");
    std::cout << queryRes->ToString();
  }
  return 0;
}
