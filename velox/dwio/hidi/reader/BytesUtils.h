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

#include <string_view>

namespace facebook::velox::hidi {

// Corresponding to Hadoop's WritableUtils::decodeVIntSize
int decodeSize(int value);

// Corresponding to HBase's PureJavaComparer
int compareTo(const char* buffer1, int length1,
              const char* buffer2, int length2);

std::string_view getText(const char* buffer, int32_t& index);

// Corresponding to org.apache.hadoop.hbase.util.ByteBufferUtils::readVLong
int64_t readVLong(const char* buffer, int32_t& index);

// Corresponding to org.apache.hadoop.hbase.util.Bytes::readAsInt
int readAsInt(const char* bytes, int offset, int length);

template <typename T>
T getNumber(const char* buffer);

template <typename T>
T getNumber(const char* buffer, int32_t& index);

} // namespace facebook::velox::hidi
