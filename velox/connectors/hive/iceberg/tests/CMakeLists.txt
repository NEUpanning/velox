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
if(NOT VELOX_DISABLE_GOOGLETEST)

  add_executable(velox_hive_iceberg_test IcebergReadTest.cpp)
  add_test(velox_hive_iceberg_test velox_hive_iceberg_test)

  target_link_libraries(
    velox_hive_iceberg_test
    velox_hive_connector
    velox_hive_iceberg_splitreader
    velox_hive_partition_function
    velox_dwio_common_exception
    velox_dwio_common_test_utils
    velox_dwio_dwrf_proto
    velox_vector_test_lib
    velox_exec
    velox_exec_test_lib
    Folly::folly
    gtest
    gtest_main)

endif()