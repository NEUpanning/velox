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

#include <algorithm>
#include <cstdint>
#include <functional>

#include "velox/dwio/hidi/reader/AutoVector.h"
#include "velox/dwio/hidi/reader/HFileReader.h"
#include "velox/dwio/hidi/reader/BytesUtils.h"

namespace facebook::velox::hidi {

const size_t kMaxSizet = std::numeric_limits<size_t>::max();

// Corresponding to HiDiKeyValueComparator
struct CellComparator {
  bool operator()(const Cell& leftCell, const Cell& rightCell) const {
    // compare rowkey
    int diff = compareTo(leftCell.rowkey, leftCell.rowkeyLen,
        rightCell.rowkey, rightCell.rowkeyLen);
    if (diff != 0) {
      return diff > 0;
    }
    if (leftCell.familyLen + leftCell.columnLen == 0 && leftCell.type == 0) {
      // left is "bigger", i.e. it appears later in the sorted order
      return true;
    }
    if (rightCell.familyLen + rightCell.columnLen == 0 && rightCell.type == 0) {
      return false;
    }

    // comparing column family is enough
    if (leftCell.familyLen != rightCell.familyLen) {
      return compareTo(leftCell.family, leftCell.familyLen,
          rightCell.family, rightCell.familyLen) > 0;
    }

    // compare family
    diff = compareTo(leftCell.family, leftCell.familyLen,
        rightCell.family, rightCell.familyLen);
    if (diff != 0) {
      return diff > 0;
    }

    // compare column
    diff = compareTo(leftCell.column, leftCell.columnLen,
        rightCell.column, rightCell.columnLen);
    if (diff != 0) {
      return diff > 0;
    }

    // compare timestamp
    if (leftCell.timestamp != rightCell.timestamp) {
      return leftCell.timestamp < rightCell.timestamp;
    }

    // compare custom mvcc
    if (leftCell.mvccType == MvccType::MYSQLV2
        && rightCell.mvccType == MvccType::MYSQLV2) {
      MysqlV2MvccInfo left;
      MysqlV2MvccInfo right;
      if (leftCell.getMysqlV2MvccInfo(left) && rightCell.getMysqlV2MvccInfo(right)) {
        diff = left.compareTo(right);
        if (diff != 0) {
          return diff > 0;
        }
      }
    }

    // compare sequenceId
    return leftCell.getSequeceId() < rightCell.getSequeceId();
  }
};

struct HFileReaderComparator {
  CellComparator comparator;
  // return true when left sorted after right
  bool operator()(HFileReader* a, HFileReader* b) const {
    return comparator(a->getCell(), b->getCell());
  }
};

// Copy from RocksDB
template<typename T, typename Compare = std::less<T>>
class BinaryHeap {
 public:
  BinaryHeap() {}
  explicit BinaryHeap(Compare cmp) : cmp_(std::move(cmp)) {}

  void push(const T& value) {
    data_.push_back(value);
    upheap(data_.size() - 1);
  }

  void push(T&& value) {
    data_.push_back(std::move(value));
    upheap(data_.size() - 1);
  }

  const T& top() const {
    assert(!empty());
    return data_.front();
  }

  void replace_top(const T& value) {
    assert(!empty());
    data_.front() = value;
    downheap(get_root());
  }

  void replace_top(T&& value) {
    assert(!empty());
    data_.front() = std::move(value);
    downheap(get_root());
  }

  void pop() {
    assert(!empty());
    if (data_.size() > 1) {
      // Avoid self-move-assign, because it could cause problems with
      // classes which are not prepared for this and it trips up the
      // STL debugger when activated.
      data_.front() = std::move(data_.back());
    }
    data_.pop_back();
    if (!empty()) {
      downheap(get_root());
    } else {
      reset_root_cmp_cache();
    }
  }

  void swap(BinaryHeap &other) {
    std::swap(cmp_, other.cmp_);
    data_.swap(other.data_);
    std::swap(root_cmp_cache_, other.root_cmp_cache_);
  }

  void clear() {
    data_.clear();
    reset_root_cmp_cache();
  }

  bool empty() const { return data_.empty(); }

  size_t size() const { return data_.size(); }

  void reset_root_cmp_cache() { root_cmp_cache_ = kMaxSizet; }

 private:
  static inline size_t get_root() { return 0; }
  static inline size_t get_parent(size_t index) { return (index - 1) / 2; }
  static inline size_t get_left(size_t index) { return 2 * index + 1; }
  static inline size_t get_right(size_t index) { return 2 * index + 2; }

  void upheap(size_t index) {
    T v = std::move(data_[index]);
    while (index > get_root()) {
      const size_t parent = get_parent(index);
      if (!cmp_(data_[parent], v)) {
        break;
      }
      data_[index] = std::move(data_[parent]);
      index = parent;
    }
    data_[index] = std::move(v);
    reset_root_cmp_cache();
  }

  void downheap(size_t index) {
    T v = std::move(data_[index]);

    size_t picked_child = kMaxSizet;
    while (1) {
      const size_t left_child = get_left(index);
      if (get_left(index) >= data_.size()) {
        break;
      }
      const size_t right_child = left_child + 1;
      assert(right_child == get_right(index));
      picked_child = left_child;
      if (index == 0 && root_cmp_cache_ < data_.size()) {
        picked_child = root_cmp_cache_;
      } else if (right_child < data_.size() &&
                 cmp_(data_[left_child], data_[right_child])) {
        picked_child = right_child;
      }
      if (!cmp_(v, data_[picked_child])) {
        break;
      }
      data_[index] = std::move(data_[picked_child]);
      index = picked_child;
    }

    if (index == 0) {
      // We did not change anything in the tree except for the value
      // of the root node, left and right child did not change, we can
      // cache that `picked_child` is the smallest child
      // so next time we compare againist it directly
      root_cmp_cache_ = picked_child;
    } else {
      // the tree changed, reset cache
      reset_root_cmp_cache();
    }

    data_[index] = std::move(v);
  }

  Compare cmp_;
  autovector<T> data_;
  // Used to reduce number of cmp_ calls in downheap()
  size_t root_cmp_cache_ = kMaxSizet;
};

} // namespace facebook::velox::hidi
