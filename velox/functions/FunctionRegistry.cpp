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
#include "velox/functions/FunctionRegistry.h"

#include <boost/algorithm/string.hpp>
#include <algorithm>
#include <iterator>
#include <optional>
#include <sstream>
#include "velox/common/base/Exceptions.h"
#include "velox/core/SimpleFunctionMetadata.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"

namespace facebook::velox {
namespace {

void populateSimpleFunctionSignatures(FunctionSignatureMap& map) {
  const auto& simpleFunctions = exec::simpleFunctions();
  for (const auto& functionName : simpleFunctions.getFunctionNames()) {
    map[functionName] = simpleFunctions.getFunctionSignatures(functionName);
  }
}

void populateVectorFunctionSignatures(FunctionSignatureMap& map) {
  auto vectorFunctions = exec::vectorFunctionFactories();
  vectorFunctions.withRLock([&map](const auto& locked) {
    for (const auto& it : locked) {
      const auto& allSignatures = it.second.signatures;
      auto& curSignatures = map[it.first];
      std::transform(
          allSignatures.begin(),
          allSignatures.end(),
          std::back_inserter(curSignatures),
          [](std::shared_ptr<exec::FunctionSignature> signature)
              -> exec::FunctionSignature* { return signature.get(); });
    }
  });
}

} // namespace

FunctionSignatureMap getFunctionSignatures() {
  FunctionSignatureMap result;
  populateSimpleFunctionSignatures(result);
  populateVectorFunctionSignatures(result);
  return result;
}

std::vector<const exec::FunctionSignature*> getFunctionSignatures(
    const std::string& functionName) {
  // Some functions have both simple and vector implementations (for different
  // signatures). Collect all signatures.
  // Check simple functions first.
  auto signatures = exec::simpleFunctions().getFunctionSignatures(functionName);

  // Check vector functions.
  auto& vectorFunctions = exec::vectorFunctionFactories();
  vectorFunctions.withRLock([&](const auto& functions) {
    auto it = functions.find(functionName);
    if (it != functions.end()) {
      for (const auto& signature : it->second.signatures) {
        signatures.push_back(signature.get());
      }
    }
  });

  return signatures;
}

FunctionSignatureMap getVectorFunctionSignatures() {
  FunctionSignatureMap result;
  populateVectorFunctionSignatures(result);
  return result;
}

void clearFunctionRegistry() {
  exec::mutableSimpleFunctions().clearRegistry();
  exec::vectorFunctionFactories().withWLock(
      [](auto& functionMap) { functionMap.clear(); });
}

std::optional<bool> isDeterministic(const std::string& functionName) {
  const auto simpleFunctions =
      exec::simpleFunctions().getFunctionSignaturesAndMetadata(functionName);
  const auto metadata = exec::getVectorFunctionMetadata(functionName);
  if (simpleFunctions.empty() && !metadata.has_value()) {
    return std::nullopt;
  }

  for (const auto& [metadata_2, _] : simpleFunctions) {
    if (!metadata_2.deterministic) {
      return false;
    }
  }
  if (metadata.has_value() && !metadata.value().deterministic) {
    return false;
  }
  return true;
}

TypePtr resolveFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  // Check if this is a simple function.
  if (auto returnType = resolveSimpleFunction(functionName, argTypes)) {
    return returnType;
  }

  // Check if VectorFunctions has this function name + signature.
  return resolveVectorFunction(functionName, argTypes);
}

TypePtr resolveFunctionWithCoercions(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes,
    std::vector<TypePtr>& coercions) {
  // Check if this is a simple function.
  if (auto resolvedFunction =
          exec::simpleFunctions().resolveFunctionWithCoercions(
              functionName, argTypes, coercions)) {
    return resolvedFunction->type();
  }

  // Check if VectorFunctions has this function name + signature.
  return exec::resolveVectorFunctionWithCoercions(
      functionName, argTypes, coercions);
}

std::optional<std::pair<TypePtr, exec::VectorFunctionMetadata>>
resolveFunctionWithMetadata(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  if (auto resolvedFunction =
          exec::simpleFunctions().resolveFunction(functionName, argTypes)) {
    return std::pair<TypePtr, exec::VectorFunctionMetadata>{
        resolvedFunction->type(), resolvedFunction->metadata()};
  }

  return exec::resolveVectorFunctionWithMetadata(functionName, argTypes);
}

TypePtr resolveFunctionOrCallableSpecialForm(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  if (auto returnType = resolveCallableSpecialForm(functionName, argTypes)) {
    return returnType;
  }

  return resolveFunction(functionName, argTypes);
}

TypePtr resolveCallableSpecialForm(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  return exec::resolveTypeForSpecialForm(functionName, argTypes);
}

TypePtr resolveSimpleFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  if (auto resolvedFunction =
          exec::simpleFunctions().resolveFunction(functionName, argTypes)) {
    return resolvedFunction->type();
  }

  return nullptr;
}

TypePtr resolveVectorFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  return exec::resolveVectorFunction(functionName, argTypes);
}

std::optional<std::pair<TypePtr, exec::VectorFunctionMetadata>>
resolveVectorFunctionWithMetadata(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes) {
  return exec::resolveVectorFunctionWithMetadata(functionName, argTypes);
}

void removeFunction(const std::string& functionName) {
  exec::mutableSimpleFunctions().removeFunction(functionName);
  exec::vectorFunctionFactories().withWLock(
      [&](auto& functionMap) { functionMap.erase(functionName); });
}

} // namespace facebook::velox
