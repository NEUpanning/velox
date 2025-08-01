# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(
  velox_functions_spark_aggregates_test
  AverageAggregationTest.cpp
  BitwiseXorAggregationTest.cpp
  BloomFilterAggAggregateTest.cpp
  CentralMomentsAggregationTest.cpp
  CollectListAggregateTest.cpp
  CollectSetAggregateTest.cpp
  CovarianceAggregatesTest.cpp
  FirstAggregateTest.cpp
  LastAggregateTest.cpp
  Main.cpp
  MinMaxAggregationTest.cpp
  MinMaxByAggregationTest.cpp
  ModeAggregateTest.cpp
  RegrReplacementAggregationTest.cpp
  SumAggregationTest.cpp
  VarianceAggregationTest.cpp)

add_test(velox_functions_spark_aggregates_test
         velox_functions_spark_aggregates_test)

target_link_libraries(
  velox_functions_spark_aggregates_test
  velox_exec_test_lib
  velox_vector_test_lib
  velox_functions_aggregates_test_lib
  velox_functions_spark
  velox_functions_spark_aggregates
  velox_hive_connector
  velox_vector_fuzzer
  gflags::gflags
  GTest::gtest
  GTest::gtest_main)
