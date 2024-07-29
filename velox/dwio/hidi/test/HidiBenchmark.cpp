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

#include <folly/Benchmark.h>
#include <folly/Varint.h>
#include <folly/init/Init.h>

#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/hidi/reader/HidiReader.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;
using namespace facebook::velox::type;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::hidi;
using namespace facebook::velox::exec::test;
using exec::test::HiveConnectorTestBase;

static std::shared_ptr<folly::Executor> intTaskEnv() {
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, std::make_shared<core::MemConfig>());
  connector::registerConnector(hiveConnector);
  filesystems::registerLocalFileSystem();
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  return std::make_shared<folly::CPUThreadPoolExecutor>(
      std::thread::hardware_concurrency());
}

static std::shared_ptr<const RowType> getSchemaType() {
  std::string schema = "struct<"
      "c0:INT,c1:BIGINT,c2:STRING,c3:BOOLEAN,c4:TINYINT,c5:SMALLINT,"
      "c6:FLOAT,c7:DOUBLE,c8:BINARY,c9:TIMESTAMP,c10:DATE,c11:DECIMAL(10,2)>";
  return asRowType(fbhive::HiveTypeParser().parse(schema));
}

static std::shared_ptr<const RowType> getOutputType(
    const std::shared_ptr<const RowType>& rowType,
    const std::vector<std::string>& projections) {
  if (projections.empty()) {
    return rowType;
  }
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto& projection : projections) {
    names.emplace_back(projection);
    types.emplace_back(rowType->findChild(projection));
  }
  return ROW(std::move(names), std::move(types));
}

static std::shared_ptr<exec::Task> initQueryTask(
    const std::vector<std::string>& projections,
    const std::string& filter,
    const std::vector<std::string>& groupingKeys = {},
    const std::vector<std::string>& aggregates = {},
    bool compactValue = true) {
  static auto executor = intTaskEnv();
  static auto pool = memory::memoryManager()->addLeafPool();
  static auto rowType = getSchemaType();
  auto outputType = getOutputType(rowType, projections);
  PlanBuilder builder(pool.get());

  core::PlanNodeId scanNode;
  builder.tableScan(
      "hive_table",
      outputType,
      {}/*columnAliases*/,
      {}/*subfieldFilters*/,
      filter/*remainingFilter*/,
      rowType/*schema*/,
      {}/*assignments*/,
      false/*filterPushdown*/).capturePlanNodeId(scanNode);
  if (!groupingKeys.empty()) {
    builder.singleAggregation(groupingKeys, aggregates);
  }

  auto planFragment = builder.planFragment();
  auto queryTask = exec::Task::create(
      "QueryTask", planFragment, 0/*destination*/,
      core::QueryCtx::create(executor.get()), exec::Task::ExecutionMode::kSerial);

  // prepare Split
  std::string current_path = fs::current_path().c_str();
  std::string filePath = current_path;
  if (compactValue) {
    filePath += "/velox/dwio/hidi/test/examples/primitive_mysqlv2";
  } else {
    filePath += "/velox/dwio/hidi/test/examples/primitive_split_value";
  }
  const size_t last_slash_idx = filePath.rfind('/');
  std::string dir = filePath.substr(0, last_slash_idx);
  std::string file = filePath.substr(last_slash_idx + 1);
  std::string splitInfo = "{\"dir\":\"file:" + dir + "\", \"files\":[\""+ file + "\"]}";

  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId, splitInfo, dwio::common::FileFormat::HIDI);
  if (!compactValue) {
    connectorSplit->customSplitInfo.emplace("compactValues", "false");
  }
  queryTask->addSplit(scanNode, exec::Split{connectorSplit});
  queryTask->noMoreSplits(scanNode);
  return queryTask;
}

BENCHMARK(CompactFullScan) {
  folly::BenchmarkSuspender suspender;
  auto task = initQueryTask({}, "");
  suspender.dismiss();
  RowVectorPtr result = task->next();
  folly::doNotOptimizeAway(result);
}

BENCHMARK_RELATIVE(SplitFullScan) {
  folly::BenchmarkSuspender suspender;
  auto task = initQueryTask({}, "", {}, {}, false);
  suspender.dismiss();
  RowVectorPtr result = task->next();
  folly::doNotOptimizeAway(result);
}

BENCHMARK(CompactScanWithFilter) {
  folly::BenchmarkSuspender suspender;
  auto task = initQueryTask({"c0","c9","c11"}, "c0 > 10");
  suspender.dismiss();
  RowVectorPtr result = task->next();
  folly::doNotOptimizeAway(result);
}

BENCHMARK_RELATIVE(SplitScanWithFilter) {
  folly::BenchmarkSuspender suspender;
  auto task = initQueryTask({"c0","c9","c11"}, "c0 > 10", {}, {}, false);
  suspender.dismiss();
  RowVectorPtr result = task->next();
  folly::doNotOptimizeAway(result);
}

BENCHMARK(CompactScanWithAggregation) {
  folly::BenchmarkSuspender suspender;
  auto task = initQueryTask({"c0","c1","c2"}, "c0 > 10", {"c2"}, {"sum(c1)"});
  suspender.dismiss();
  RowVectorPtr result = task->next();
  folly::doNotOptimizeAway(result);
}

BENCHMARK_RELATIVE(SplitScanWithAggregation) {
  folly::BenchmarkSuspender suspender;
  auto task = initQueryTask({"c0","c1","c2"}, "c0 > 10", {"c2"}, {"sum(c1)"}, false);
  suspender.dismiss();
  RowVectorPtr result = task->next();
  folly::doNotOptimizeAway(result);
}

int32_t main(int32_t argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}

/*
============================================================================
[...]elox/dwio/hidi/test/HidiBenchmark.cpp     relative  time/iter   iters/s
============================================================================
CompactFullScan                                           474.29us     2.11K
SplitFullScan                                   85.719%   553.31us     1.81K
CompactScanWithFilter                                     274.99us     3.64K
SplitScanWithFilter                             83.535%   329.19us     3.04K
CompactScanWithAggregation                                505.66us     1.98K
SplitScanWithAggregation                        91.111%   554.99us     1.80K
============================================================================
*/
