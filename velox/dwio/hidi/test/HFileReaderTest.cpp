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

#include "velox/common/file/File.h"
#include "velox/common/memory/MemoryPool.h"
#include "dwio/hidi/reader/Compression.h"
#include "velox/dwio/hidi/reader/HFileReader.h"
#include "velox/dwio/hidi/reader/BytesUtils.h"
#include "velox/dwio/hidi/test/HidiTestBase.h"
#include "velox/dwio/hidi/proto/hfile.pb.h"
#include <google/protobuf/util/delimited_message_util.h>
#include <lzo/lzo1x.h>

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::hidi;
using namespace google::protobuf::util;

namespace {
  auto defaultPool = memory::addDefaultLeafMemoryPool();
  CompressionOptions options;
}

class HFileReaderTest : public HidiTestBase {
 public:
  std::unique_ptr<RowReader> createRowReader(
      const std::string& fileName,
      const RowTypePtr& rowType) {
    return nullptr;
  }

  std::shared_ptr<LocalReadFile> getSampleFile() {
    std::string current_path = fs::current_path().c_str();
    std::string filePath = current_path + "/velox/dwio/hidi/test/examples/sample_hfile";
    return std::make_shared<LocalReadFile>(filePath);
  }

  std::shared_ptr<HFileReader> getSampleReader() {
    auto readFile = getSampleFile();
    options.format.lz4_lzo.isHadoopFrameFormat = true;
    options.format.lz4_lzo.strategy = 12; // lzo1x
    return std::make_shared<HFileReader>(readFile, *defaultPool, options);
  }
};

TEST_F(HFileReaderTest, parseTrailer) {
  auto readFile = getSampleFile();
  auto input = std::make_unique<BufferedInput>(readFile, *defaultPool);
  uint64_t offset = readFile->size() - HFileReader::kTrailerSize + HFileReader::kTrailerMagicLen;
  auto inputStream = input->read(offset, HFileReader::kTrailerProtoLen, LogType::FOOTER);
  auto trailerProto = std::make_unique<proto::FileTrailerProto>();
  DWIO_ENSURE(
      ParseDelimitedFromZeroCopyStream(trailerProto.get(), inputStream.get(), nullptr),
      "Failed to parse Trailer from ",
      inputStream->getName());
  printf("[file_info_offset] %ld\n", trailerProto->file_info_offset());
  EXPECT_EQ(trailerProto->file_info_offset(), 2529788);
  printf("[load_on_open_data_offset] %ld\n", trailerProto->load_on_open_data_offset());
  EXPECT_EQ(trailerProto->load_on_open_data_offset(), 2529632);
  printf("[uncompressed_data_index_size] %ld\n", trailerProto->uncompressed_data_index_size());
  EXPECT_EQ(trailerProto->uncompressed_data_index_size(), 67);
  printf("[total_uncompressed_bytes] %ld\n", trailerProto->total_uncompressed_bytes());
  EXPECT_EQ(trailerProto->total_uncompressed_bytes(), 5585382);
  printf("[data_index_count] %d\n", trailerProto->data_index_count());
  EXPECT_EQ(trailerProto->data_index_count(), 1);
  printf("[meta_index_count] %d\n", trailerProto->meta_index_count());
  EXPECT_EQ(trailerProto->meta_index_count(), 0);
  printf("[entry_count] %ld\n", trailerProto->entry_count());
  EXPECT_EQ(trailerProto->entry_count(), 17550);
  printf("[num_data_index_levels] %d\n", trailerProto->num_data_index_levels());
  EXPECT_EQ(trailerProto->num_data_index_levels(), 1);
  printf("[first_data_block_offset] %ld\n", trailerProto->first_data_block_offset());
  EXPECT_EQ(trailerProto->first_data_block_offset(), 0);
  printf("[last_data_block_offset] %ld\n", trailerProto->last_data_block_offset());
  EXPECT_EQ(trailerProto->last_data_block_offset(), 0);
  EXPECT_EQ(trailerProto->comparator_class_name(),
      "com.meituan.hidi.hadoop.data.HiDiKeyValueComparator");
  printf("[compression_codec] %d\n", trailerProto->compression_codec());
  EXPECT_EQ(trailerProto->compression_codec(), 0);
}

TEST_F(HFileReaderTest, parseFileInfo) {
  auto readFile = getSampleFile();
  auto input = std::make_unique<BufferedInput>(readFile, *defaultPool);
  uint64_t fileInfoOff = 2529788;
  uint64_t len = readFile->size() - HFileReader::kTrailerSize - fileInfoOff;
  auto inputStream = input->read(fileInfoOff, HFileReader::KHeaderSize, LogType::BLOCK);
  auto headerBuffer = std::make_shared<DataBuffer<char>>(
      *defaultPool, HFileReader::KHeaderSize);
  inputStream->readFully(headerBuffer->data(), HFileReader::KHeaderSize);
  char* headerData = headerBuffer->data();
  EXPECT_EQ(std::string(headerData, 8), "FILEINF2");
  int32_t compressedSize = getNumber<int32_t>(headerData + 8);
  int32_t uncompressedSize = getNumber<int32_t>(headerData + 12);

  CompressionOptions options;
  options.format.lz4_lzo.isHadoopFrameFormat = true;
  options.format.lz4_lzo.strategy = 12; // lzo1x
  inputStream = std::make_unique<SeekableFileInputStream>(
      std::make_shared<ReadFileInputStream>(std::move(readFile)),
      fileInfoOff + HFileReader::KHeaderSize, compressedSize,
      *defaultPool, LogType::BLOCK, compressedSize/*IO unit*/);
  auto decompressedStream = createDecompressor(
      CompressionKind_LZO, std::move(inputStream), uncompressedSize,
      *defaultPool, options, "HFileBlock", nullptr/*decrypter*/,
      true/*useRawDecompression*/, compressedSize);
  auto blockBuffer = std::make_shared<DataBuffer<char>>(*defaultPool, uncompressedSize);
  decompressedStream->readFully(blockBuffer->data(), uncompressedSize);
  EXPECT_EQ(std::string(blockBuffer->data(), 4), "PBUF");
  uint8_t* blockData = reinterpret_cast<uint8_t*>(blockBuffer->data());
  auto fileInfoProto = std::make_unique<proto::FileInfoProto>();
  google::protobuf::io::CodedInputStream codedInput(blockData + 4, uncompressedSize);
  DWIO_ENSURE(
      ParseDelimitedFromCodedStream(fileInfoProto.get(), &codedInput, nullptr),
      "Failed to parse FileInfo");
  for (int i = 0; i < fileInfoProto->map_entry_size(); i++) {
    auto& pair = fileInfoProto->map_entry(i);
    printf("%s -> %s\n", pair.first().c_str(), pair.second().c_str());
  }
}

TEST_F(HFileReaderTest, parseBlockHeader) {
  auto readFile = getSampleFile();
  auto input = std::make_unique<BufferedInput>(readFile, *defaultPool);
  auto inputStream = input->read(
      0/*first_data_block_offset*/, HFileReader::KHeaderSize, LogType::BLOCK);
  auto headerBuffer = std::make_shared<DataBuffer<char>>(
      *defaultPool, HFileReader::KHeaderSize);
  inputStream->readFully(headerBuffer->data(), HFileReader::KHeaderSize);
  char* headerData = headerBuffer->data();
  // blockType
  EXPECT_EQ(std::string(headerData, 8), "DATABLK*");
  // compressedSizeWithoutHeader
  EXPECT_EQ(getNumber<int32_t>(headerData + 8), 2529599);
  // uncompressedSizeWithoutHeader
  EXPECT_EQ(getNumber<int32_t>(headerData + 12), 5580764);
  // preBlockOffset
  EXPECT_EQ(getNumber<int64_t>(headerData + 16), -1l);
  // sizeWithoutChecksum
  EXPECT_EQ(getNumber<int32_t>(headerData + 29), 2529012);
}

TEST_F(HFileReaderTest, blockIter) {
//  std::string filePath = "/velox/hidi_hfile_150M";
//  auto readFile = std::make_shared<LocalReadFile>(filePath);
  auto readFile = getSampleFile();
  auto input = std::make_unique<BufferedInput>(readFile, *defaultPool);

  auto headerStream = input->read(0, HFileReader::KHeaderSize, LogType::BLOCK);
  auto headerBuffer = std::make_shared<DataBuffer<char>>(
      *defaultPool, HFileReader::KHeaderSize);
  headerStream->readFully(headerBuffer->data(), HFileReader::KHeaderSize);
  char* headerData = headerBuffer->data();
  int32_t blockSize = getNumber<int32_t>(headerData + 8);

  int32_t endOffset = 2529632; // load on open data offset
  int32_t index = HFileReader::KHeaderSize;
  auto buffer = std::make_shared<DataBuffer<char>>(
      *defaultPool, blockSize + HFileReader::KHeaderSize);
  while (index < endOffset) {
    int32_t size = blockSize + HFileReader::KHeaderSize;
    buffer->reserve(size);
    auto inputStream = input->read(index, size, LogType::BLOCK);
    inputStream->readFully(buffer->data(), size);
    headerData = buffer->data() + blockSize;
    blockSize = getNumber<int32_t>(headerData + 8);
    index += size;
  }
  EXPECT_EQ(std::string(headerData, 8), "IDXROOT2");
}

TEST_F(HFileReaderTest, lzo1xDecompress) {
  uint64_t blockSize = 2529599;
  auto readFile = getSampleFile();
  auto input = std::make_unique<BufferedInput>(readFile, *defaultPool);
  auto inputStream = input->read(33/*offset*/, blockSize, LogType::BLOCK);

  auto uncompressedBuf = std::make_shared<DataBuffer<unsigned char>>(
      *defaultPool, 262144/*bufferSize*/);
  auto wrkmem = std::make_shared<DataBuffer<char>>(*defaultPool, LZO1X_1_MEM_COMPRESS);
  auto compressedBuf = std::make_shared<DataBuffer<char>>(*defaultPool, blockSize);
  inputStream->readFully(compressedBuf->data(), blockSize);
  char* blockData = compressedBuf->data();

  int r = lzo_init();
  EXPECT_EQ(r, LZO_E_OK);

  uint64_t targetSize = getNumber<int32_t>(blockData);
  uint64_t index = 4;
  uint64_t uncompressedSize = 0;

  uint64_t chunkSize;
  uint64_t chunkuncompressedSize;
  while (uncompressedSize < targetSize) {
    chunkSize = getNumber<int32_t>(blockData + index);
    index += 4;
    r = lzo1x_decompress((const unsigned char*)blockData + index, chunkSize,
                         uncompressedBuf->data(), &chunkuncompressedSize,
                         wrkmem->data());
    EXPECT_EQ(r, LZO_E_OK);
    uncompressedSize += chunkuncompressedSize;
    index += chunkSize;
  }
  EXPECT_EQ(targetSize, uncompressedSize);
}

TEST_F(HFileReaderTest, parseDataBlock) {
  uint64_t compressedSize = 2529599;
  uint64_t uncompressedSize = 5580764;
  auto readFile = getSampleFile();
  auto inputStream = std::make_unique<SeekableFileInputStream>(
      std::make_shared<ReadFileInputStream>(std::move(readFile)),
      33/*offset*/, compressedSize, *defaultPool, LogType::BLOCK, compressedSize/*IO unit*/);

  CompressionOptions options;
  options.format.lz4_lzo.isHadoopFrameFormat = true;
  options.format.lz4_lzo.strategy = 12; // lzo1x

  auto decompressedStream = createDecompressor(
      CompressionKind_LZO, std::move(inputStream), uncompressedSize,
      *defaultPool, options, "HFileBlock", nullptr/*decrypter*/,
      true/*useRawDecompression*/, compressedSize);

  auto blockBuffer = std::make_shared<DataBuffer<char>>(*defaultPool, uncompressedSize);
  decompressedStream->readFully(blockBuffer->data(), uncompressedSize);
  char* blockData = blockBuffer->data();

  int entryCount = 0;
  uint64_t index = 0;
  while (index < uncompressedSize) {
    int32_t keyLen = getNumber<int32_t>(blockData + index);
    index += 4;

    int32_t valLen = getNumber<int32_t>(blockData + index);
    index += 4;

    int16_t rowKeyLen = getNumber<int16_t>(blockData + index);
    index += 2;
    index += rowKeyLen;

    int8_t colLen = getNumber<int8_t>(blockData + index);
    index += 1;
    index += colLen;

    int64_t timestamp = getNumber<int64_t>(blockData + index);
    index += 8;

    int8_t keyType = getNumber<int8_t>(blockData + index);
    index += 1;

    index += valLen;
    int16_t tagsLen = getNumber<int16_t>(blockData + index);
    index += 2;
    index += tagsLen;

    int mvccLen = decodeSize(getNumber<int8_t>(blockData + index));
    index += mvccLen;

    entryCount++;
  }
  EXPECT_EQ(entryCount, 17550);
}

TEST_F(HFileReaderTest, TestSeek) {
  auto reader = getSampleReader();
  Scan scan;
  EXPECT_TRUE(reader->seek(scan));
  EXPECT_THROW(Scan("456", "123"), VeloxException);
  Scan scan2("123");
  EXPECT_TRUE(reader->seek(scan2));
}

TEST_F(HFileReaderTest, TestFullScan) {
  auto reader = getSampleReader();
  uint64_t entryCount = 0;
  Scan scan;
  reader->seek(scan);
  MysqlV2MvccInfo mvccInfo;
  while (reader->next()) {
    entryCount++;
    EXPECT_TRUE(reader->getCell().getMysqlV2MvccInfo(mvccInfo));
  }
  EXPECT_EQ(entryCount, reader->getEntryCount());
}
