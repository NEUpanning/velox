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

#include "BytesUtils.h"
#include <folly/lang/Bits.h>

namespace {

// Corresponding to Hadoop's WritableUtils::isNegativeVInt
inline bool isNegative(int value) {
  return value < -120 || (value >= -112 && value < 0);
}

} // namespace

namespace facebook::velox::hidi {

// Corresponding to Hadoop's WritableUtils::decodeVIntSize
int decodeSize(int value) {
  if (value >= -112) {
    return 1;
  } else if (value < -120) {
    return -119 - value;
  }
  return -111 - value;
}

// Corresponding to HBase's PureJavaComparer
int compareTo(const char* buffer1, int length1,
              const char* buffer2, int length2) {
  int res = std::memcmp(buffer1, buffer2, std::min(length1, length2));
  return res == 0 ? (length1 - length2) : res;
}

std::string_view getText(const char* buffer, int32_t& index) {
  int64_t length;
  int firstByte = buffer[index++];
  int len = decodeSize(firstByte);
  if (len == 1) {
    length = firstByte;
  } else {
    long i = 0;
    for (int idx = 0; idx < len - 1; idx++) {
      unsigned char b = buffer[index++];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    length = (isNegative(firstByte) ? (i ^ -1L) : i);
  }
  std::string_view retval(buffer + index, length);
  index += length;
  return retval;
}

// Corresponding to org.apache.hadoop.hbase.util.ByteBufferUtils::readVLong
int64_t readVLong(const char* buffer, int32_t& index) {
  int firstByte = buffer[index++];
  int len = decodeSize(firstByte);
  if (len == 1) {
    return firstByte;
  } else {
    long value = 0;
    for (int idx = 1; idx < len; idx++) {
      char b = buffer[index++];
      value <<= 8;
      value |= (b & 0xFF);
    }
    return isNegative(firstByte) ? ~value : value;
  }
}

// Corresponding to org.apache.hadoop.hbase.util.Bytes::readAsInt
int readAsInt(const char* bytes, int offset, int length) {
  int n = 0;
  for (int i = offset; i < (offset + length); i++) {
    n <<= 8;
    n ^= bytes[i] & 0xFF;
  }
  return n;
}

template <typename T>
T getNumber(const char* buffer) {
  return folly::Endian::big(folly::loadUnaligned<T>(buffer));
}

template <typename T>
T getNumber(const char* buffer, int32_t& index) {
  T val = folly::Endian::big(folly::loadUnaligned<T>(buffer + index));
  index += sizeof(T);
  return val;
}

template signed char getNumber(const char* buffer);
template short getNumber(const char* buffer);
template int getNumber(const char* buffer);
template long getNumber(const char* buffer);

template signed char getNumber(const char* buffer, int32_t& index);
template short getNumber(const char* buffer, int32_t& index);
template int getNumber(const char* buffer, int32_t& index);
template long getNumber(const char* buffer, int32_t& index);

} // namespace facebook::velox::hidi
