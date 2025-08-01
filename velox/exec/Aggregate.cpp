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

#include "velox/exec/Aggregate.h"

#include <unordered_map>
#include "velox/exec/AggregateCompanionAdapter.h"
#include "velox/exec/AggregateCompanionSignatures.h"
#include "velox/exec/AggregateWindow.h"
#include "velox/expression/SignatureBinder.h"

namespace facebook::velox::exec {

bool isRawInput(core::AggregationNode::Step step) {
  return step == core::AggregationNode::Step::kPartial ||
      step == core::AggregationNode::Step::kSingle;
}

bool isPartialOutput(core::AggregationNode::Step step) {
  return step == core::AggregationNode::Step::kPartial ||
      step == core::AggregationNode::Step::kIntermediate;
}

bool isPartialInput(core::AggregationNode::Step step) {
  return step == core::AggregationNode::Step::kIntermediate ||
      step == core::AggregationNode::Step::kFinal;
}

AggregateFunctionMap& aggregateFunctions() {
  static AggregateFunctionMap functions;
  return functions;
}

const AggregateFunctionEntry* FOLLY_NULLABLE
getAggregateFunctionEntry(const std::string& name) {
  auto sanitizedName = sanitizeName(name);

  return aggregateFunctions().withRLock(
      [&](const auto& functionsMap) -> const AggregateFunctionEntry* {
        auto it = functionsMap.find(sanitizedName);
        if (it != functionsMap.end()) {
          return &it->second;
        }
        return nullptr;
      });
}

AggregateRegistrationResult registerAggregateFunction(
    const std::string& name,
    const std::vector<std::shared_ptr<AggregateFunctionSignature>>& signatures,
    const AggregateFunctionFactory& factory,
    const AggregateFunctionMetadata& metadata,
    bool registerCompanionFunctions,
    bool overwrite) {
  auto sanitizedName = sanitizeName(name);
  AggregateRegistrationResult registered;

  if (overwrite) {
    aggregateFunctions().withWLock([&](auto& aggregationFunctionMap) {
      aggregationFunctionMap[sanitizedName] = {
          signatures, std::move(factory), metadata};
    });
    registered.mainFunction = true;
  } else {
    auto inserted =
        aggregateFunctions().withWLock([&](auto& aggregationFunctionMap) {
          auto [_, inserted_2] = aggregationFunctionMap.insert(
              {sanitizedName, {signatures, factory, metadata}});
          return inserted_2;
        });
    registered.mainFunction = inserted;
  }

  // If the aggregate is not a companion function, also register it as a window
  // function.
  if (!metadata.companionFunction) {
    registerAggregateWindowFunction(sanitizedName);
  }

  // Register companion function if needed.
  if (registerCompanionFunctions) {
    auto companionMetadata = metadata;
    companionMetadata.companionFunction = true;

    registered.partialFunction =
        CompanionFunctionsRegistrar::registerPartialFunction(
            name, signatures, companionMetadata, overwrite);
    registered.mergeFunction =
        CompanionFunctionsRegistrar::registerMergeFunction(
            name, signatures, companionMetadata, overwrite);
    registered.extractFunction =
        CompanionFunctionsRegistrar::registerExtractFunction(
            name, signatures, overwrite);
    registered.mergeExtractFunction =
        CompanionFunctionsRegistrar::registerMergeExtractFunction(
            name, signatures, companionMetadata, overwrite);
  }
  return registered;
}

AggregateRegistrationResult registerAggregateFunction(
    const std::string& name,
    const std::vector<std::shared_ptr<AggregateFunctionSignature>>& signatures,
    const AggregateFunctionFactory& factory,
    bool registerCompanionFunctions,
    bool overwrite) {
  return registerAggregateFunction(
      name, signatures, factory, {}, registerCompanionFunctions, overwrite);
}

std::vector<AggregateRegistrationResult> registerAggregateFunction(
    const std::vector<std::string>& names,
    const std::vector<std::shared_ptr<AggregateFunctionSignature>>& signatures,
    const AggregateFunctionFactory& factory,
    bool registerCompanionFunctions,
    bool overwrite) {
  return registerAggregateFunction(
      names, signatures, factory, {}, registerCompanionFunctions, overwrite);
}

std::vector<AggregateRegistrationResult> registerAggregateFunction(
    const std::vector<std::string>& names,
    const std::vector<std::shared_ptr<AggregateFunctionSignature>>& signatures,
    const AggregateFunctionFactory& factory,
    const AggregateFunctionMetadata& metadata,
    bool registerCompanionFunctions,
    bool overwrite) {
  auto size = names.size();
  std::vector<AggregateRegistrationResult> registrationResults{size};
  for (int i = 0; i < size; ++i) {
    registrationResults[i] = registerAggregateFunction(
        names[i],
        signatures,
        factory,
        metadata,
        registerCompanionFunctions,
        overwrite);
  }
  return registrationResults;
}

const AggregateFunctionMetadata& getAggregateFunctionMetadata(
    const std::string& name) {
  const auto sanitizedName = sanitizeName(name);
  if (auto func = getAggregateFunctionEntry(sanitizedName)) {
    return func->metadata;
  }
  VELOX_USER_FAIL("Aggregate function not found: {}", name);
}

std::unordered_map<
    std::string,
    std::vector<std::shared_ptr<AggregateFunctionSignature>>>
getAggregateFunctionSignatures() {
  std::unordered_map<
      std::string,
      std::vector<std::shared_ptr<AggregateFunctionSignature>>>
      map;
  exec::aggregateFunctions().withRLock([&](const auto& aggregateFunctions) {
    for (const auto& aggregateFunction : aggregateFunctions) {
      map[aggregateFunction.first] = aggregateFunction.second.signatures;
    }
  });

  return map;
}

std::optional<std::vector<std::shared_ptr<AggregateFunctionSignature>>>
getAggregateFunctionSignatures(const std::string& name) {
  if (auto func = getAggregateFunctionEntry(name)) {
    return func->signatures;
  }

  return std::nullopt;
}

namespace {

// return a vector of one single CompanionSignatureEntry instance {name,
// signatues}.
std::vector<CompanionSignatureEntry> getCompanionSignatures(
    std::string&& name,
    std::vector<AggregateFunctionSignaturePtr>&& signatures) {
  std::vector<CompanionSignatureEntry> entries;
  entries.push_back(
      {std::move(name),
       std::vector<FunctionSignaturePtr>{
           signatures.begin(), signatures.end()}});
  return entries;
}

std::vector<CompanionSignatureEntry> getCompanionSignatures(
    std::string&& name,
    std::vector<FunctionSignaturePtr>&& signatures) {
  std::vector<CompanionSignatureEntry> entries;
  entries.push_back({std::move(name), std::move(signatures)});
  return entries;
}

// Process original signatures grouped by return type and construct new
// signatures through `getNewSignatures`. For each signature group, construct a
// companion function name with suffix of the return type via `getNewName`.
// Finally, add a vector of the companion function names and their signatures to
// signatureMap at the key `companionType`.
template <typename T>
std::vector<CompanionSignatureEntry> getCompanionSignaturesWithSuffix(
    const std::string& name,
    const std::vector<AggregateFunctionSignaturePtr>& signatures,
    const std::function<std::vector<T>(
        const std::vector<AggregateFunctionSignaturePtr>&)>& getNewSignatures,
    const std::function<std::string(const std::string&, const TypeSignature&)>&
        getNewName) {
  std::vector<CompanionSignatureEntry> entries;
  auto groupedSignatures =
      CompanionSignatures::groupSignaturesByReturnType(signatures);
  for (const auto& [type, signatureGroup] : groupedSignatures) {
    auto newSignatures = getNewSignatures(signatureGroup);
    if (newSignatures.empty()) {
      continue;
    }

    if constexpr (std::is_same_v<T, FunctionSignaturePtr>) {
      entries.push_back({getNewName(name, type), std::move(newSignatures)});
    } else {
      entries.push_back(
          {getNewName(name, type),
           std::vector<FunctionSignaturePtr>{
               newSignatures.begin(), newSignatures.end()}});
    }
  }
  return entries;
}

} // namespace

std::optional<CompanionFunctionSignatureMap> getCompanionFunctionSignatures(
    const std::string& name) {
  auto* entry = getAggregateFunctionEntry(name);
  if (!entry) {
    return std::nullopt;
  }

  const auto& signatures = entry->signatures;
  CompanionFunctionSignatureMap companionSignatures;

  companionSignatures.partial = getCompanionSignatures(
      CompanionSignatures::partialFunctionName(name),
      CompanionSignatures::partialFunctionSignatures(signatures));

  companionSignatures.merge = getCompanionSignatures(
      CompanionSignatures::mergeFunctionName(name),
      CompanionSignatures::mergeFunctionSignatures(signatures));

  if (CompanionSignatures::hasSameIntermediateTypesAcrossSignatures(
          signatures)) {
    companionSignatures.extract =
        getCompanionSignaturesWithSuffix<FunctionSignaturePtr>(
            name,
            signatures,
            CompanionSignatures::extractFunctionSignatures,
            CompanionSignatures::extractFunctionNameWithSuffix);
    companionSignatures.mergeExtract =
        getCompanionSignaturesWithSuffix<AggregateFunctionSignaturePtr>(
            name,
            signatures,
            CompanionSignatures::mergeExtractFunctionSignatures,
            CompanionSignatures::mergeExtractFunctionNameWithSuffix);
  } else {
    companionSignatures.extract = getCompanionSignatures(
        CompanionSignatures::extractFunctionName(name),
        CompanionSignatures::extractFunctionSignatures(signatures));
    companionSignatures.mergeExtract = getCompanionSignatures(
        CompanionSignatures::mergeExtractFunctionName(name),
        CompanionSignatures::mergeExtractFunctionSignatures(signatures));
  }
  return companionSignatures;
}

std::unique_ptr<Aggregate> Aggregate::create(
    const std::string& name,
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& resultType,
    const core::QueryConfig& config) {
  // TODO(timaou, kletkavrubashku): Reneable the validation once "regr_slope"
  // signature is fixed
  //
  // Validate the result type. if (isPartialOutput(step)) {
  //   auto intermediateType = Aggregate::intermediateType(name, argTypes);
  //   VELOX_CHECK(
  //       resultType->equivalent(*intermediateType),
  //       "Intermediate type mismatch. Expected: {}, actual: {}",
  //       intermediateType->toString(),
  //       resultType->toString());
  // } else {
  //   auto finalType = Aggregate::finalType(name, argTypes);
  //   VELOX_CHECK(
  //       resultType->equivalent(*finalType),
  //       "Final type mismatch. Expected: {}, actual: {}",
  //       finalType->toString(),
  //       resultType->toString());
  // }
  // Lookup the function in the new registry first.
  if (auto func = getAggregateFunctionEntry(name)) {
    return func->factory(step, argTypes, resultType, config);
  }

  VELOX_USER_FAIL("Aggregate function not registered: {}", name);
}

void Aggregate::setLambdaExpressions(
    std::vector<core::LambdaTypedExprPtr> lambdaExpressions,
    std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator) {
  lambdaExpressions_ = std::move(lambdaExpressions);
  expressionEvaluator_ = std::move(expressionEvaluator);
}

void Aggregate::setAllocatorInternal(HashStringAllocator* allocator) {
  allocator_ = allocator;
}

void Aggregate::setOffsetsInternal(
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    int32_t initializedByte,
    uint8_t initializedMask,
    int32_t rowSizeOffset) {
  nullByte_ = nullByte;
  nullMask_ = nullMask;
  initializedByte_ = initializedByte;
  initializedMask_ = initializedMask;
  offset_ = offset;
  rowSizeOffset_ = rowSizeOffset;
}

void Aggregate::clearInternal() {
  numNulls_ = 0;
}

void Aggregate::singleInputAsIntermediate(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    VectorPtr& result) const {
  VELOX_CHECK_EQ(args.size(), 1);
  const auto& input = args[0];
  if (rows.isAllSelected()) {
    result = input;
    return;
  }
  VELOX_CHECK_NOT_NULL(result);
  // Set result to NULL for rows that are masked out.
  {
    auto nulls = allocateNulls(rows.size(), allocator_->pool(), bits::kNull);
    rows.clearNulls(nulls);
    result->setNulls(nulls);
  }
  result->copy(input.get(), rows, nullptr);
}

} // namespace facebook::velox::exec
