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
add_subdirectory(utils)

add_executable(
  velox_vector_test
  CopyPreserveEncodingsTest.cpp
  DecodedVectorTest.cpp
  EncodedVectorCopyTest.cpp
  EncodingTest.cpp
  EnsureWritableVectorTest.cpp
  FlatMapVectorTest.cpp
  IsWritableVectorTest.cpp
  LazyVectorTest.cpp
  MayHaveNullsRecursiveTest.cpp
  SelectivityVectorTest.cpp
  StringVectorBufferTest.cpp
  VariantToVectorTest.cpp
  VectorCompareTest.cpp
  VectorEstimateFlatSizeTest.cpp
  VectorMakerTest.cpp
  VectorPoolTest.cpp
  VectorPrepareForReuseTest.cpp
  VectorPrinterTest.cpp
  VectorSaverTest.cpp
  VectorStreamTest.cpp
  VectorTest.cpp
  VectorTestUtils.cpp
  VectorToStringTest.cpp)

add_test(velox_vector_test velox_vector_test)

target_link_libraries(
  velox_vector_test
  velox_time
  velox_vector_fuzzer
  velox_vector_test_lib
  velox_buffer
  velox_serialization
  velox_presto_serializer
  velox_presto_types
  velox_temp_path
  velox_type_test_lib
  Boost::atomic
  Boost::context
  Boost::date_time
  Boost::filesystem
  Boost::program_options
  Boost::regex
  Boost::thread
  Boost::system
  GTest::gtest
  GTest::gtest_main
  GTest::gmock
  Folly::folly
  glog::glog
  fmt::fmt
  dl)

add_executable(simple_vector_test SimpleVectorTest.cpp ToStringUtilityTest.cpp)

add_test(simple_vector_test simple_vector_test)

target_link_libraries(
  simple_vector_test
  velox_vector
  velox_buffer
  velox_type
  velox_vector_test_lib
  GTest::gtest
  GTest::gtest_main
  Folly::folly
  glog::glog
  fmt::fmt)

add_executable(bias_vector_test BiasVectorTest.cpp)

add_test(bias_vector_test bias_vector_test)

target_link_libraries(
  bias_vector_test
  velox_vector
  Boost::filesystem
  GTest::gtest
  GTest::gtest_main
  Folly::folly
  glog::glog
  fmt::fmt)
