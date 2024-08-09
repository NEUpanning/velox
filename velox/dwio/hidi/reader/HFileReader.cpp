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

#include "velox/dwio/hidi/reader/HFileReader.h"
#include "velox/dwio/hidi/reader/BytesUtils.h"
#include "velox/dwio/hidi/proto/hfile.pb.h"
#include <google/protobuf/util/delimited_message_util.h>

using namespace google::protobuf::util;

namespace facebook::velox::hidi {

Scan::Scan(const std::string& start, const std::string& end)
    : startKey(start), endKey(end) {
  if (!start.empty() && !end.empty()) {
    DWIO_ENSURE_LE(
        compareTo(startKey.data(), startKey.size(), endKey.data(), endKey.size()),
        0, "startKey {} larger than endKey {}", startKey, endKey);
  }
}

int MysqlV2MvccInfo::compareTo(const MysqlV2MvccInfo& other) const {
  long diff = executeTime - other.executeTime;
  if (diff != 0) {
    return diff > 0 ? -1 : 1;
  }
  diff = sourceIpGlobalId - other.sourceIpGlobalId;
  if (diff != 0) {
    return diff > 0 ? -1 : 1;
  }
  diff = binlogFileIndex - other.binlogFileIndex;
  if (diff != 0) {
    return diff > 0 ? -1 : 1;
  }
  diff = binlogFileOffset - other.binlogFileOffset;
  if (diff != 0) {
    return diff > 0 ? -1 : 1;
  }
  diff = changeSetLineSeq - other.changeSetLineSeq;
  if (diff != 0) {
    return diff > 0 ? -1 : 1;
  }
  return 0;
}

bool Cell::getMysqlV2MvccInfo(MysqlV2MvccInfo& mvccInfo) const {
  int pos = 0;
  while (pos < tagsLen) {
    int tagLen = readAsInt(tags, pos, HFileReader::KTagLenSize);
    char type = tags[pos + HFileReader::KTagLenSize];
    if (type == HFileReader::KMvccTagType) {
      int offset = pos + HFileReader::KTagLenSize + 1/*typeLen*/;
      mvccInfo.executeTime = readVLong(tags, offset);
      mvccInfo.sourceIpGlobalId = readVLong(tags, offset);
      mvccInfo.binlogFileIndex = readVLong(tags, offset);
      mvccInfo.binlogFileOffset = readVLong(tags, offset);
      mvccInfo.changeSetLineSeq = readVLong(tags, offset);
      return true;
	}
    pos += HFileReader::KTagLenSize + tagLen;
  }
  return false;
}

// Corresponding to HBase's HFileScannerImpl::readMvccVersion
long Cell::getSequeceId() const {
  if (mvccLen > 0) {
    int offset = 0;
    return readVLong(mvcc, offset);
  } else {
    return 0;
  }
}

HFileReader::HFileReader(
    std::shared_ptr<ReadFile> file,
    memory::MemoryPool& pool,
    hidi::CompressionOptions& options,
    std::shared_ptr<DataBuffer<char>> buffer)
    : pool_(pool), options_(options), input_(std::make_shared<ReadFileInputStream>(file)),
      fileSize_(file->size()), fileName_(file->getName()) {
  blockBuffer = std::make_unique<DataBuffer<char>>(pool_, KHeaderSize);
  tempBuffer = (buffer != nullptr ? buffer : std::make_shared<DataBuffer<char>>(pool_, KHeaderSize));
  cell_.mvccType = MvccType::MYSQLV2;
  reset();
}

uint64_t HFileReader::getEntryCount() const {
  DWIO_ENSURE_NE(startOffset_, -1, "Should call Seek first!");
  return entryCount_;
}

uint64_t HFileReader::getTotalSize() const {
  DWIO_ENSURE_NE(startOffset_, -1, "Should call Seek first!");
  return totalSizes_;
}

void HFileReader::decompresseBlock(int32_t compressedSize, int32_t uncompressedSize) {
  auto decompressor = hidi::createDecompressor(
      compressionKind_, uncompressedSize, options_, "HFileBlock");
  if (decompressor) {
    decompressor->decompress(
        tempBuffer->data(), compressedSize, blockBuffer->data(), uncompressedSize);
  } else {
    blockBuffer->append(0, tempBuffer->data(), compressedSize);
  }
}

bool HFileReader::resetBlock() {
  bool res = false;
  int32_t readSize = nextCompressedSize_ + KHeaderSize;
  // read block data
  tempBuffer->reserve(readSize);
  input_->read(tempBuffer->data(), readSize, startOffset_, LogType::FILE);
  startOffset_ += readSize;
  if (nextDataBlock_) {
    curBlockIndex_ = 0;
    curBlockSize_ = nextUncompressedSize_;
    blockBuffer->reserve(curBlockSize_);
    decompresseBlock(nextCompressedSize_, curBlockSize_);
    res = true;
  }
  // reset next block header
  char* blockHeader = tempBuffer->data() + nextCompressedSize_;
  nextDataBlock_ = (std::string_view(blockHeader, KCompressedSizeOff) == kDataBlock);
  nextCompressedSize_ = getNumber<int32_t>(blockHeader + KCompressedSizeOff);
  nextUncompressedSize_ = getNumber<int32_t>(blockHeader + KUncompressedSizeOff);

  needCompareEndkey_ = endKey_.empty() ? false : (startOffset_ >= endOffset_ ? true : false);
  return res;
}

// Corresponding to HBase's locateNonRootIndexEntry
int HFileReader::locateNonRootIndexEntry(const char* blockData) const {
  int32_t numEntries = getNumber<int32_t>(blockData);
  int low = 0;
  int high = numEntries - 1;
  int mid = 0;
  int entriesOffset = 4 * (numEntries + 2);
  bool find = false;
  while (low <= high) {
    mid = (low + high) / 2;
    int32_t midKeyRelOffset = getNumber<int32_t>(blockData + 4 * (mid + 1));
    int midKeyOffset = entriesOffset + midKeyRelOffset + 12/*skip offset and size*/;
    int midLength = getNumber<int32_t>(blockData + 4 * (mid + 2)) - midKeyRelOffset - 12;
    int cmp = compareTo(blockData + midKeyOffset, midLength, startKey_.data(), startKey_.size());
    if (cmp < 0) { // startKey after currentKey
      low = mid + 1;
    } else if (cmp > 0) { // startKey before currentKey
      high = mid - 1;
    } else {
      find = true;
      break;
    }
  }
  if (!find) {
    if (low != high + 1) {
      DWIO_RAISE("Binary search broken: low={} instead of {}", low, high + 1);
    }
    mid = low - 1;
    if (mid < -1 || mid >= numEntries) {
      DWIO_RAISE("Binary search broken: "
          "result is {} but expected to be between -1 and (numEntries - 1) = {}",
          mid, numEntries - 1);
    }
    if (mid == -1) {
      DWIO_RAISE("The key {} is before the first key of the non-root index block", startKey_);
    }
  }
  int entryRelOffset = getNumber<int32_t>(blockData + 4 * (1 + mid));
  return entriesOffset + entryRelOffset;
}

void HFileReader::loadDataBlock(
    int64_t& offset, int32_t compressedSize, int32_t uncompressedSize) {
  std::string_view blockType = kLeafIndex;
  while (blockType != kDataBlock) {
    DWIO_ENSURE(
        blockType == kLeafIndex || blockType == kMidIndex,
        "{} is not an index block",
        blockType);
    // decompress index block
    tempBuffer->reserve(compressedSize);
    blockBuffer->reserve(uncompressedSize);
    input_->read(tempBuffer->data(), compressedSize, offset, LogType::FILE);
    decompresseBlock(compressedSize, uncompressedSize);
    char* blockData = blockBuffer->data();
    int entryOffset = locateNonRootIndexEntry(blockData);
    offset = getNumber<int64_t>(blockData + entryOffset);
    input_->read(tempBuffer->data(), KHeaderSize, offset, LogType::FILE);
    offset += KHeaderSize;
    char* blockHeader = tempBuffer->data();
    blockType = std::string_view(blockHeader, KCompressedSizeOff);
    compressedSize = getNumber<int32_t>(blockHeader + KCompressedSizeOff);
    uncompressedSize = getNumber<int32_t>(blockHeader + KUncompressedSizeOff);
  }
  // reset next block header
  nextDataBlock_ = true;
  nextCompressedSize_ = compressedSize;
  nextUncompressedSize_ = uncompressedSize;
}

bool HFileReader::loadFirstBlock() {
  if (endOffset_ < startOffset_) {
    LOG(WARNING) << fileName_ << "'s startOffset(" << startOffset_
                 << ") larger than endOffset(" << endOffset_ << ")";
    return false;
  }
  // start block header
  input_->read(tempBuffer->data(), KHeaderSize, startOffset_, LogType::FILE);
  char* blockHeader = tempBuffer->data();
  nextDataBlock_ = (std::string_view(blockHeader, KCompressedSizeOff) == kDataBlock);
  nextCompressedSize_ = getNumber<int32_t>(blockHeader + KCompressedSizeOff);
  nextUncompressedSize_ = getNumber<int32_t>(blockHeader + KUncompressedSizeOff);

  startOffset_ += KHeaderSize;
  endOffset_ += KHeaderSize;

  if (!nextDataBlock_) { // multi-level index
    LOG(WARNING) << "target hfile " << fileName_ << " has multi-levels";
    int32_t compressedSize = getNumber<int32_t>(blockHeader + KCompressedSizeOff);
    int32_t uncompressedSize = getNumber<int32_t>(blockHeader + KUncompressedSizeOff);
    if (endOffset_ != rootIndexOffset_ + KHeaderSize) {
      // deal with endOffset_ first, or it will reset blockHeader
      input_->read(tempBuffer->data(), KHeaderSize, endOffset_ - KHeaderSize, LogType::FILE);
      blockHeader = tempBuffer->data();
      loadDataBlock(
        endOffset_,
        getNumber<int32_t>(blockHeader + KCompressedSizeOff),
        getNumber<int32_t>(blockHeader + KUncompressedSizeOff));
    }
    // search index block to get target startOffset_
    loadDataBlock(startOffset_, compressedSize, uncompressedSize);
  }

  LOG(WARNING) << fileName_ << " read range determined, start offset "
            << startOffset_ << ", end offset " << endOffset_;
  return true;
}

bool HFileReader::seek(const Scan& scan) {
  reset();
  startKey_ = scan.startKey;
  endKey_ = scan.endKey;
  needCompareStartkey_ = startKey_.empty() ? false : true;
  BufferedInput buffInput(input_, pool_);
  uint64_t offset = 0;
  uint64_t length = fileSize_;
  if (fileSize_ > kFooterSize) {
    offset = fileSize_ - kFooterSize;
    length = kFooterSize;
  }
  buffInput.enqueue({offset, length});
  buffInput.load(LogType::FOOTER);

  // parse trailer
  uint64_t trailerOffset = fileSize_ - kTrailerSize + kTrailerMagicLen;
  auto inputStream = buffInput.read(trailerOffset, kTrailerProtoLen, LogType::FOOTER);
  auto trailerProto = std::make_unique<proto::FileTrailerProto>();
  DWIO_ENSURE(
      ParseDelimitedFromZeroCopyStream(trailerProto.get(), inputStream.get(), nullptr),
      "Failed to parse Trailer from ",
      inputStream->getName());
  rootIndexOffset_ = trailerProto->load_on_open_data_offset();
  compressionKind_ = getCompressionKind(trailerProto->compression_codec());
  indexMultiLevel_ = trailerProto->num_data_index_levels() > 1;
  entryCount_ = trailerProto->entry_count();
  totalSizes_ = trailerProto->total_uncompressed_bytes();
  startOffset_ = trailerProto->first_data_block_offset();
  endOffset_ = rootIndexOffset_;

  // parse FileInfo
  uint64_t fileInfoOff = trailerProto->file_info_offset();
  uint64_t len = fileSize_ - kTrailerSize - fileInfoOff;
  inputStream = buffInput.read(fileInfoOff, len, LogType::FOOTER);
  inputStream->readFully(blockBuffer->data(), KHeaderSize);
  char* headerData = blockBuffer->data();
  int32_t compressedSize = getNumber<int32_t>(headerData + KCompressedSizeOff);
  int32_t uncompressedSize = getNumber<int32_t>(headerData + KUncompressedSizeOff);
  inputStream = buffInput.read(fileInfoOff + KHeaderSize, compressedSize, LogType::BLOCK);
  auto decompressedStream = hidi::createDecompressor(
      compressionKind_, std::move(inputStream), uncompressedSize,
      pool_, options_, "HFileBlock", nullptr/*decrypter*/,
      true/*useRawDecompression*/, compressedSize);
  blockBuffer->reserve(uncompressedSize);
  decompressedStream->readFully(blockBuffer->data(), uncompressedSize);
  uint8_t* fileInfoData = reinterpret_cast<uint8_t*>(blockBuffer->data());
  google::protobuf::io::CodedInputStream codedInput(
      fileInfoData + 4/*Skip Magic Word*/, uncompressedSize);
  auto fileInfoProto = std::make_unique<proto::FileInfoProto>();
  DWIO_ENSURE(
      ParseDelimitedFromCodedStream(fileInfoProto.get(), &codedInput, nullptr),
      "Failed to parse FileInfo");
  // Corresponding to Java's HidiHFileReader
  for (int i = 0; i < fileInfoProto->map_entry_size(); i++) {
    auto& pair = fileInfoProto->map_entry(i);
    if (pair.first() == kMvccType) {
      if (pair.second() == kSeqBaseMvcc) {
        cell_.mvccType = MvccType::SEQID_BASE;
      }
      continue;
    }
    if (pair.first() == kColomnTypes) {
      fileSchema_ = pair.second();
      continue;
    }
  }

  // No need to parse index data, if we do full scan
  if (scan.fullScan()) {
    return loadFirstBlock();
  }

  // parse ROOT_INDEX
  inputStream = buffInput.read(rootIndexOffset_, KHeaderSize, LogType::HEADER);
  inputStream->readFully(blockBuffer->data(), KHeaderSize);
  headerData = blockBuffer->data();
  compressedSize = getNumber<int32_t>(headerData + KCompressedSizeOff);
  uncompressedSize = getNumber<int32_t>(headerData + KUncompressedSizeOff);
  inputStream = buffInput.read(rootIndexOffset_ + KHeaderSize, compressedSize, LogType::BLOCK);
  decompressedStream = hidi::createDecompressor(
      compressionKind_, std::move(inputStream), uncompressedSize,
      pool_, options_, "HFileBlock", nullptr/*decrypter*/,
      true/*useRawDecompression*/, compressedSize);
  blockBuffer->reserve(uncompressedSize);
  decompressedStream->readFully(blockBuffer->data(), uncompressedSize);
  const char* blockData = blockBuffer->data();

  bool startDone = startKey_.empty() ? true : false;
  bool endKeyEmpty = endKey_.empty();
  int32_t index = 0;
  while (index < uncompressedSize) {
    int64_t offset = getNumber<int64_t>(blockData, index);
    index += 4; // skip blockSize field
    std::string_view rowkey = getText(blockData, index);
    int16_t rowLen = getNumber<int16_t>(rowkey.data());
    if (!startDone) {
      int res = compareTo(rowkey.data() + KRowKeyOffset, rowLen,
          startKey_.data(), startKey_.size());
      if (res < 0) { // startKey after currentKey
        startOffset_ = offset;
      } else {
        startDone = true;
        if (res == 0) {
          startOffset_ = offset;
        }
        if (endKeyEmpty) {
          break;
        }
      }
    }
    if (!endKeyEmpty) {
      int res = compareTo(rowkey.data() + KRowKeyOffset, rowLen,
          endKey_.data(), endKey_.size());
      if (res <= 0) { // endKey after or equal currentKey
        endOffset_ = offset;
      } else { // endKey before currentKey
        if (endOffset_ == rootIndexOffset_) {
          LOG(WARNING) << "skip " << fileName_ << " since there is no data in target key range("
                       << startKey_ << " ~ " << endKey_ << ")";
          return false;
        }
        break; // range determined
      }
    }
  }
  return loadFirstBlock();
}

bool HFileReader::next() {
  DWIO_ENSURE_NE(startOffset_, -1, "Should call Seek first.");
  while (startOffset_ <= rootIndexOffset_ + KHeaderSize) {
    // if current block reach end, skip to next block
    if (curBlockIndex_ == curBlockSize_ && !resetBlock()) {
      continue;
    }

    const char* blockData = blockBuffer->data();
    int32_t keyLength = getNumber<int32_t>(blockData, curBlockIndex_);
    cell_.valLen = getNumber<int32_t>(blockData, curBlockIndex_);
    cell_.rowkeyLen = getNumber<int16_t>(blockData, curBlockIndex_);
    cell_.rowkey = blockData + curBlockIndex_;
    curBlockIndex_ += cell_.rowkeyLen;

    cell_.familyLen = getNumber<int8_t>(blockData, curBlockIndex_);
    cell_.family = blockData + curBlockIndex_;
    curBlockIndex_ += cell_.familyLen;

    cell_.columnLen = keyLength - KRowInfraSize - cell_.rowkeyLen - cell_.familyLen;
    cell_.column = blockData + curBlockIndex_;
    curBlockIndex_ += cell_.columnLen;

    cell_.timestamp = getNumber<int64_t>(blockData, curBlockIndex_);
    cell_.type = getNumber<int8_t>(blockData, curBlockIndex_);
    cell_.value = blockData + curBlockIndex_;
    curBlockIndex_ += cell_.valLen;

    cell_.tagsLen = getNumber<int16_t>(blockData, curBlockIndex_);
    cell_.tags = blockData + curBlockIndex_;
    curBlockIndex_ += cell_.tagsLen;

    cell_.mvccLen = decodeSize(getNumber<int8_t>(blockData + curBlockIndex_));
    cell_.mvcc = blockData + curBlockIndex_;
    curBlockIndex_ += cell_.mvccLen;

    if (needCompareStartkey_) {
      if (compareTo(cell_.rowkey, cell_.rowkeyLen,
          startKey_.data(), startKey_.size()) < 0) {
        // current key before start key, skip to next
        continue;
      }
      // no need to compare every Cell with startKey_
      needCompareStartkey_ = false;
    }

    if (needCompareEndkey_ &&
        compareTo(cell_.rowkey, cell_.rowkeyLen,
            endKey_.data(), endKey_.size()) > 0) {
      LOG(WARNING) << "currentKey " << std::string(cell_.rowkey, cell_.rowkeyLen)
                   << " after endKey " << endKey_ << ", " << fileName_ << " reach end.";
      return false;
    }
    return true;
  }
  LOG(WARNING) << "target HFile " << fileName_ << " reach end.";
  return false; // reach end
}

} // namespace facebook::velox::hidi
