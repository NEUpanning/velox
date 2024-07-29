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

#include "velox/common/compression/Compression.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/encryption/Encryption.h"
#include "velox/dwio/common/compression/Compression.h"

namespace facebook::velox::hidi {

struct CompressionOptions {
  /// Format specific compression/decompression options
  union Format {
    struct {
      /// Window bits determines the history buffer size and whether
      /// header/trailer is added to the compression block.
      int windowBits;
      /// Compression level determines the compression ratio. Zlib supports
      /// values ranging from 0 (no compression) to 9 (max compression)
      int32_t compressionLevel;
    } zlib;

    struct {
      int32_t compressionLevel;
    } zstd;

    struct {
      bool isHadoopFrameFormat;
      /// LZO1A(1), LZO1X(12)
      int32_t strategy;
    } lz4_lzo;
  } format;

  uint32_t compressionThreshold;
};

/**
 * Create a decompressor for the given compression kind.
 * @param kind The compression type to implement
 * @param options The compression options to use
 */
std::unique_ptr<dwio::common::compression::Decompressor> createDecompressor(
    facebook::velox::common::CompressionKind kind,
    uint64_t blockSize,
    const CompressionOptions& options,
    const std::string& streamDebugInfo);

/**
 * Create a decompressor for the given compression kind.
 * @param kind The compression type to implement
 * @param input The input stream that is the underlying source
 * @param bufferSize The maximum size of the buffer
 * @param pool The memory pool
 * @param options The compression options to use
 * @param useRawDecompression Specify whether to perform raw decompression
 * @param compressedLength The compressed block length for raw decompression
 */
std::unique_ptr<dwio::common::SeekableInputStream> createDecompressor(
    facebook::velox::common::CompressionKind kind,
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    uint64_t bufferSize,
    memory::MemoryPool& pool,
    const CompressionOptions& options,
    const std::string& streamDebugInfo,
    const dwio::common::encryption::Decrypter* decryptr = nullptr,
    bool useRawDecompression = false,
    size_t compressedLength = 0);

} // namespace facebook::velox::hidi
