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

#pragma once

#include <gtest/gtest.h>
#include <string>
#include "velox/common/base/Fs.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::hidi {

class HidiTestBase : public testing::Test, public test::VectorTestBase {
 protected:
  void SetUp() override {
    dwio::common::LocalFileSink::registerFactory();
    rootPool_ = memory::defaultMemoryManager().addRootPool("HidiTests");
    leafPool_ = rootPool_->addLeafChild("HidiTests");
    tempPath_ = exec::test::TempDirectoryPath::create();
  }

  static RowTypePtr sampleSchema() {
    return ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  }

  dwio::common::RowReaderOptions getReaderOpts(
      const RowTypePtr& rowType,
      bool fileColumnNamesReadAsLowerCase = false) {
    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
            rowType,
            rowType->names(),
            nullptr,
            fileColumnNamesReadAsLowerCase));
    return rowReaderOpts;
  }

  std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
      const RowTypePtr& rowType) {
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
    scanSpec->addAllChildFields(*rowType);
    return scanSpec;
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return test::getDataFilePath(
        "velox/dwio/hidi/test", "examples/" + fileName);
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;
};
} // namespace facebook::velox::hidi
