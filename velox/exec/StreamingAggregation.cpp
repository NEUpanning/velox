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

#include "velox/exec/StreamingAggregation.h"

namespace facebook::velox::exec {

StreamingAggregation::StreamingAggregation(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::AggregationNode>& aggregationNode)
    : Operator(
          driverCtx,
          aggregationNode->outputType(),
          operatorId,
          aggregationNode->id(),
          aggregationNode->step() == core::AggregationNode::Step::kPartial
              ? "PartialAggregation"
              : "Aggregation"),
      maxOutputBatchSize_{outputBatchRows()},
      minOutputBatchSize_{
          operatorCtx_->driverCtx()
                      ->queryConfig()
                      .streamingAggregationMinOutputBatchRows() > 0
              ? std::min(
                    maxOutputBatchSize_,
                    operatorCtx_->driverCtx()
                        ->queryConfig()
                        .streamingAggregationMinOutputBatchRows())
              : maxOutputBatchSize_},
      aggregationNode_{aggregationNode},
      step_{aggregationNode->step()} {
  if (aggregationNode_->ignoreNullKeys()) {
    VELOX_UNSUPPORTED(
        "Streaming aggregation doesn't support ignoring null keys yet");
  }
}

void StreamingAggregation::initialize() {
  Operator::initialize();

  auto numKeys = aggregationNode_->groupingKeys().size();
  decodedKeys_.resize(numKeys);

  auto inputType = aggregationNode_->sources()[0]->outputType();

  std::vector<TypePtr> groupingKeyTypes;
  groupingKeyTypes.reserve(numKeys);

  groupingKeys_.reserve(numKeys);
  for (const auto& key : aggregationNode_->groupingKeys()) {
    auto channel = exprToChannel(key.get(), inputType);
    groupingKeys_.push_back(channel);
    groupingKeyTypes.push_back(inputType->childAt(channel));
  }

  std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator;
  aggregates_ = toAggregateInfo(
      *aggregationNode_, *operatorCtx_, numKeys, expressionEvaluator, true);

  // Setup SortedAggregations.
  sortedAggregations_ =
      SortedAggregations::create(aggregates_, inputType, pool());

  distinctAggregations_.reserve(aggregates_.size());
  for (auto& aggregate : aggregates_) {
    if (aggregate.distinct) {
      distinctAggregations_.emplace_back(
          DistinctAggregations::create({&aggregate}, inputType, pool()));
    } else {
      distinctAggregations_.push_back(nullptr);
    }
  }

  if (isRawInput(step_)) {
    for (column_index_t i = 0; i < aggregates_.size(); ++i) {
      if (aggregates_[i].sortingKeys.empty() && !aggregates_[i].distinct) {
        // Must be set before we initialize row container, because it could
        // change the type and size of accumulator.
        aggregates_[i].function->setClusteredInput(true);
      }
    }
  }
  masks_ = std::make_unique<AggregationMasks>(extractMaskChannels(aggregates_));
  rows_ = makeRowContainer(groupingKeyTypes);

  initializeAggregates(numKeys);

  aggregationNode_.reset();
}

void StreamingAggregation::close() {
  if (rows_ != nullptr) {
    rows_->clear();
  }
  Operator::close();
}

void StreamingAggregation::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

namespace {
// Compares a row in one vector with another row in another vector and returns
// true if two rows match in all grouping key columns.
bool equalKeys(
    const std::vector<column_index_t>& keys,
    const RowVectorPtr& batch,
    vector_size_t index,
    const RowVectorPtr& otherBatch,
    vector_size_t otherIndex) {
  for (auto key : keys) {
    if (!batch->childAt(key)->equalValueAt(
            otherBatch->childAt(key).get(), index, otherIndex)) {
      return false;
    }
  }

  return true;
}
} // namespace

char* StreamingAggregation::startNewGroup(vector_size_t index) {
  if (numGroups_ < groups_.size()) {
    auto* group = groups_[numGroups_++];
    rows_->initializeRow(group, true);
    storeKeys(group, index);
    return group;
  }

  auto* newGroup = rows_->newRow();
  storeKeys(newGroup, index);

  groups_.resize(numGroups_ + 1);
  groups_[numGroups_++] = newGroup;
  return newGroup;
}

void StreamingAggregation::storeKeys(char* group, vector_size_t index) {
  for (auto i = 0; i < groupingKeys_.size(); ++i) {
    rows_->store(decodedKeys_[i], index, group, i);
  }
}

RowVectorPtr StreamingAggregation::createOutput(size_t numGroups) {
  auto output = BaseVector::create<RowVector>(outputType_, numGroups, pool());

  for (auto i = 0; i < groupingKeys_.size(); ++i) {
    rows_->extractColumn(groups_.data(), numGroups, i, output->childAt(i));
  }

  const auto numKeys = groupingKeys_.size();
  for (auto i = 0; i < aggregates_.size(); ++i) {
    const auto& aggregate = aggregates_.at(i);
    if (!aggregate.sortingKeys.empty()) {
      continue;
    }

    if (aggregate.distinct) {
      continue;
    }

    const auto& function = aggregate.function;
    auto& result = output->childAt(numKeys + i);
    if (isPartialOutput(step_)) {
      function->extractAccumulators(groups_.data(), numGroups, &result);
    } else {
      function->extractValues(groups_.data(), numGroups, &result);
    }
  }

  if (sortedAggregations_) {
    sortedAggregations_->extractValues(
        folly::Range<char**>(groups_.data(), numGroups), output);
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->extractValues(
          folly::Range<char**>(groups_.data(), numGroups), output);
    }
  }

  std::rotate(groups_.begin(), groups_.begin() + numGroups, groups_.end());
  numGroups_ -= numGroups;

  return output;
}

bool StreamingAggregation::assignGroups() {
  const auto numInput = input_->size();
  VELOX_CHECK_GT(numInput, 0);

  inputGroups_.resize(numInput);

  bool prevGroupAssigned{false};
  // Look for the end of the last group.
  vector_size_t index = 0;
  if (prevInput_ != nullptr) {
    const auto prevIndex = prevInput_->size() - 1;
    auto* prevGroup = groups_[numGroups_ - 1];
    for (; index < numInput; ++index) {
      if (equalKeys(groupingKeys_, prevInput_, prevIndex, input_, index)) {
        prevGroupAssigned = true;
        inputGroups_[index] = prevGroup;
      } else {
        break;
      }
    }
  }

  if (index < numInput) {
    for (auto i = 0; i < groupingKeys_.size(); ++i) {
      decodedKeys_[i].decode(*input_->childAt(groupingKeys_[i]), inputRows_);
    }

    auto* newGroup = startNewGroup(index);
    inputGroups_[index] = newGroup;

    for (auto i = index + 1; i < numInput; ++i) {
      if (equalKeys(groupingKeys_, input_, index, input_, i)) {
        inputGroups_[i] = inputGroups_[index];
      } else {
        newGroup = startNewGroup(i);
        inputGroups_[i] = newGroup;
        index = i;
      }
    }
  }

  groupBoundaries_.clear();
  for (vector_size_t i = 1; i < numInput; ++i) {
    if (inputGroups_[i] != inputGroups_[i - 1]) {
      groupBoundaries_.push_back(i);
    }
  }
  groupBoundaries_.push_back(numInput);
  return prevGroupAssigned;
}

const SelectivityVector& StreamingAggregation::getSelectivityVector(
    size_t aggregateIndex) const {
  auto* rows = masks_->activeRows(aggregateIndex);

  // No mask? Use the current selectivity vector for this aggregation.
  return rows ? *rows : inputRows_;
}

void StreamingAggregation::evaluateAggregates() {
  for (auto i = 0; i < aggregates_.size(); ++i) {
    const auto& aggregate = aggregates_.at(i);
    if (!aggregate.sortingKeys.empty()) {
      continue;
    }

    const auto& rows = getSelectivityVector(i);
    if (!rows.hasSelections()) {
      continue;
    }

    if (aggregate.distinct) {
      distinctAggregations_.at(i)->addInput(inputGroups_.data(), input_, rows);
      continue;
    }

    const auto& function = aggregate.function;
    const auto& inputs = aggregate.inputs;
    const auto& constantInputs = aggregate.constantInputs;

    std::vector<VectorPtr> args;
    for (auto j = 0; j < inputs.size(); ++j) {
      if (inputs[j] == kConstantChannel) {
        args.push_back(
            BaseVector::wrapInConstant(input_->size(), 0, constantInputs[j]));
      } else {
        args.push_back(input_->childAt(inputs[j]));
      }
    }

    if (isRawInput(step_)) {
      if (function->supportsAddRawClusteredInput()) {
        function->addRawClusteredInput(
            inputGroups_.data(), rows, args, groupBoundaries_);
      } else {
        function->addRawInput(inputGroups_.data(), rows, args, false);
      }
    } else {
      function->addIntermediateResults(inputGroups_.data(), rows, args, false);
    }
  }

  if (sortedAggregations_) {
    sortedAggregations_->addInput(inputGroups_.data(), input_);
  }
}

bool StreamingAggregation::startDrain() {
  VELOX_CHECK(isDraining());
  VELOX_CHECK(!noMoreInput_);
  if (!input_ && numGroups_ == 0) {
    return false;
  }
  return true;
}

void StreamingAggregation::maybeFinishDrain() {
  if (FOLLY_LIKELY((numGroups_ != 0) || !isDraining())) {
    return;
  }
  prevInput_ = nullptr;
  Operator::finishDrain();
}

bool StreamingAggregation::isFinished() {
  return noMoreInput_ && input_ == nullptr && numGroups_ == 0;
}

RowVectorPtr StreamingAggregation::getOutput() {
  if (!input_) {
    SCOPE_EXIT {
      outputFirstGroup_ = false;
    };
    if ((noMoreInput_ || isDraining()) && numGroups_ > 0) {
      return createOutput(numGroups_);
    }
    if (outputFirstGroup_) {
      VELOX_CHECK_GT(numGroups_, 1);
      return createOutput(1);
    }
    maybeFinishDrain();
    return nullptr;
  }
  VELOX_CHECK(!outputFirstGroup_);

  const auto numInput = input_->size();
  inputRows_.resize(numInput);
  inputRows_.setAll();

  masks_->addInput(input_, inputRows_);

  const auto numPrevGroups = numGroups_;
  const bool prevGroupAssigned = assignGroups();
  initializeNewGroups(numPrevGroups);
  evaluateAggregates();

  RowVectorPtr output;

  if ((numPrevGroups != 0) && (numGroups_ > minOutputBatchSize_)) {
    size_t numOutputGroups{0};
    // NOTE: we only want to apply the single group output optimization if
    // 'minOutputBatchSize_' is set to one for eagerly streaming output
    // producing.
    if (!prevGroupAssigned || numPrevGroups == 1 || minOutputBatchSize_ != 1) {
      numOutputGroups = std::min(numGroups_ - 1, numPrevGroups);
    } else {
      numOutputGroups = std::min(numGroups_ - 1, numPrevGroups - 1);
      outputFirstGroup_ = (numGroups_ - numOutputGroups) > 1;
    }
    VELOX_CHECK_GT(numOutputGroups, 0);
    output = createOutput(numOutputGroups);
  }
  prevInput_ = input_;
  input_ = nullptr;
  return output;
}

std::unique_ptr<RowContainer> StreamingAggregation::makeRowContainer(
    const std::vector<TypePtr>& groupingKeyTypes) {
  std::vector<Accumulator> accumulators;
  accumulators.reserve(aggregates_.size());
  for (const auto& aggregate : aggregates_) {
    accumulators.emplace_back(
        aggregate.function.get(), aggregate.intermediateType);
  }

  if (sortedAggregations_ != nullptr) {
    accumulators.push_back(sortedAggregations_->accumulator());
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      accumulators.push_back(aggregation->accumulator());
    }
  }

  return std::make_unique<RowContainer>(
      groupingKeyTypes,
      !aggregationNode_->ignoreNullKeys(),
      accumulators,
      std::vector<TypePtr>{},
      false,
      false,
      false,
      false,
      pool());
}

void StreamingAggregation::initializeNewGroups(size_t numPrevGroups) {
  if (numGroups_ == numPrevGroups) {
    return;
  }

  std::vector<vector_size_t> newGroups;
  newGroups.resize(numGroups_ - numPrevGroups);
  std::iota(newGroups.begin(), newGroups.end(), numPrevGroups);

  for (auto i = 0; i < aggregates_.size(); ++i) {
    const auto& aggregate = aggregates_.at(i);
    if (!aggregate.sortingKeys.empty()) {
      continue;
    }

    if (aggregate.distinct) {
      distinctAggregations_.at(i)->initializeNewGroups(
          groups_.data(), newGroups);
      continue;
    }

    aggregate.function->initializeNewGroups(groups_.data(), newGroups);
  }

  if (sortedAggregations_) {
    sortedAggregations_->initializeNewGroups(groups_.data(), newGroups);
  }
}

void StreamingAggregation::initializeAggregates(uint32_t numKeys) {
  int32_t columnIndex = numKeys;
  for (auto& aggregate : aggregates_) {
    auto& function = aggregate.function;
    function->setAllocator(&rows_->stringAllocator());

    const auto rowColumn = rows_->columnAt(columnIndex);
    function->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rowColumn.initializedByte(),
        rowColumn.initializedMask(),
        rows_->rowSizeOffset());
    columnIndex++;
  }

  if (sortedAggregations_) {
    sortedAggregations_->setAllocator(&rows_->stringAllocator());
    const auto& rowColumn = rows_->columnAt(columnIndex);
    sortedAggregations_->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rowColumn.initializedByte(),
        rowColumn.initializedMask(),
        rows_->rowSizeOffset());
    columnIndex++;
  }

  for (const auto& aggregation : distinctAggregations_) {
    if (aggregation != nullptr) {
      aggregation->setAllocator(&rows_->stringAllocator());

      const auto& rowColumn = rows_->columnAt(columnIndex);
      aggregation->setOffsets(
          rowColumn.offset(),
          rowColumn.nullByte(),
          rowColumn.nullMask(),
          rowColumn.initializedByte(),
          rowColumn.initializedMask(),
          rows_->rowSizeOffset());
      columnIndex++;
    }
  }
};

} // namespace facebook::velox::exec
