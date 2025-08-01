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

#include "velox/exec/Driver.h"

#include "velox/common/process/TraceContext.h"
#include "velox/exec/Task.h"
#include "velox/vector/LazyVector.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {
namespace {

// Checks if output channel is produced using identity projection and returns
// input channel if so.
std::optional<column_index_t> getIdentityProjection(
    const std::vector<IdentityProjection>& projections,
    column_index_t outputChannel) {
  for (const auto& projection : projections) {
    if (projection.outputChannel == outputChannel) {
      return projection.inputChannel;
    }
  }
  return std::nullopt;
}

thread_local DriverThreadContext* driverThreadCtx{nullptr};

void recordSilentThrows(Operator& op) {
  auto numThrow = threadNumVeloxThrow();
  if (numThrow > 0) {
    op.stats().wlock()->addRuntimeStat(
        "numSilentThrow", RuntimeCounter(numThrow));
  }
}

// Used to generate context for exceptions that are thrown while executing an
// operator. Eg output: 'Operator: FilterProject(1) PlanNodeId: 1 TaskId:
// test_cursor_1 PipelineId: 0 DriverId: 0 OperatorAddress: 0x61a000003c80'
std::string addContextOnException(
    VeloxException::Type exceptionType,
    void* arg) {
  if (exceptionType != VeloxException::Type::kSystem) {
    return "";
  }
  auto* op = static_cast<Operator*>(arg);
  return fmt::format("Operator: {}", op->toString());
}

std::exception_ptr makeException(
    const std::string& message,
    const char* file,
    int line,
    const char* function) {
  return std::make_exception_ptr(VeloxRuntimeError(
      file,
      line,
      function,
      "",
      message,
      error_source::kErrorSourceRuntime,
      error_code::kInvalidState,
      false));
}

} // namespace

DriverCtx::DriverCtx(
    std::shared_ptr<Task> _task,
    int _driverId,
    int _pipelineId,
    uint32_t _splitGroupId,
    uint32_t _partitionId)
    : driverId(_driverId),
      pipelineId(_pipelineId),
      splitGroupId(_splitGroupId),
      partitionId(_partitionId),
      task(std::move(_task)),
      threadDebugInfo({task->queryCtx()->queryId(), task->taskId(), nullptr}) {}

const core::QueryConfig& DriverCtx::queryConfig() const {
  return task->queryCtx()->queryConfig();
}

const std::optional<TraceConfig>& DriverCtx::traceConfig() const {
  return task->traceConfig();
}

velox::memory::MemoryPool* DriverCtx::addOperatorPool(
    const core::PlanNodeId& planNodeId,
    const std::string& operatorType) {
  return task->addOperatorPool(
      planNodeId, splitGroupId, pipelineId, driverId, operatorType);
}

std::optional<common::SpillConfig> DriverCtx::makeSpillConfig(
    int32_t operatorId) const {
  const auto& queryConfig = task->queryCtx()->queryConfig();
  if (!queryConfig.spillEnabled()) {
    return std::nullopt;
  }
  if (task->spillDirectory().empty() && !task->hasCreateSpillDirectoryCb()) {
    return std::nullopt;
  }
  common::GetSpillDirectoryPathCB getSpillDirPathCb =
      [this]() -> std::string_view {
    return task->getOrCreateSpillDirectory();
  };
  const auto& spillFilePrefix =
      fmt::format("{}_{}_{}", pipelineId, driverId, operatorId);
  common::UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb =
      [this](uint64_t bytes) {
        task->queryCtx()->updateSpilledBytesAndCheckLimit(bytes);
      };
  return common::SpillConfig(
      std::move(getSpillDirPathCb),
      std::move(updateAndCheckSpillLimitCb),
      spillFilePrefix,
      queryConfig.maxSpillFileSize(),
      queryConfig.spillWriteBufferSize(),
      queryConfig.spillReadBufferSize(),
      task->queryCtx()->spillExecutor(),
      queryConfig.minSpillableReservationPct(),
      queryConfig.spillableReservationGrowthPct(),
      queryConfig.spillStartPartitionBit(),
      queryConfig.spillNumPartitionBits(),
      queryConfig.maxSpillLevel(),
      queryConfig.maxSpillRunRows(),
      queryConfig.writerFlushThresholdBytes(),
      queryConfig.spillCompressionKind(),
      queryConfig.spillPrefixSortEnabled()
          ? std::optional<common::PrefixSortConfig>(prefixSortConfig())
          : std::nullopt,
      queryConfig.spillFileCreateConfig());
}

std::atomic_uint64_t BlockingState::numBlockedDrivers_{0};

BlockingState::BlockingState(
    std::shared_ptr<Driver> driver,
    ContinueFuture&& future,
    Operator* op,
    BlockingReason reason)
    : driver_(std::move(driver)),
      future_(std::move(future)),
      operator_(op),
      reason_(reason),
      sinceUs_(std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::high_resolution_clock::now().time_since_epoch())
                   .count()) {
  // Set before leaving the thread.
  driver_->state().hasBlockingFuture = true;
  numBlockedDrivers_++;
}

// static
void BlockingState::setResume(std::shared_ptr<BlockingState> state) {
  VELOX_CHECK(!state->driver_->isOnThread());
  auto& exec = folly::QueuedImmediateExecutor::instance();
  std::move(state->future_)
      .via(&exec)
      .thenValue([state](auto&& /* unused */) {
        auto& driver = state->driver_;
        auto& task = driver->task();

        std::lock_guard<std::timed_mutex> l(task->mutex());
        if (!driver->state().isTerminated) {
          state->operator_->recordBlockingTime(state->sinceUs_, state->reason_);
        }
        VELOX_CHECK(!driver->state().suspended());
        VELOX_CHECK(driver->state().hasBlockingFuture);
        driver->state().hasBlockingFuture = false;
        if (task->pauseRequested()) {
          // The thread will be enqueued at resume.
          return;
        }
        Driver::enqueue(state->driver_);
      })
      .thenError(
          folly::tag_t<std::exception>{}, [state](std::exception const& e) {
            try {
              VELOX_FAIL(
                  "A ContinueFuture for task {} was realized with error: {}",
                  state->driver_->task()->taskId(),
                  e.what());
            } catch (const VeloxException&) {
              state->driver_->task()->setError(std::current_exception());
            }
          });
}

std::string stopReasonString(StopReason reason) {
  switch (reason) {
    case StopReason::kNone:
      return "NONE";
    case StopReason::kBlock:
      return "BLOCK";
    case StopReason::kTerminate:
      return "TERMINATE";
    case StopReason::kAlreadyTerminated:
      return "ALREADY_TERMINATED";
    case StopReason::kYield:
      return "YIELD";
    case StopReason::kPause:
      return "PAUSE";
    case StopReason::kAlreadyOnThread:
      return "ALREADY_ON_THREAD";
    case StopReason::kAtEnd:
      return "AT_END";
    default:
      return fmt::format("UNKNOWN_REASON {}", static_cast<int>(reason));
  }
}

std::ostream& operator<<(std::ostream& out, const StopReason& reason) {
  return out << stopReasonString(reason);
}

// static
void Driver::enqueue(std::shared_ptr<Driver> driver) {
  process::ScopedThreadDebugInfo scopedInfo(
      driver->driverCtx()->threadDebugInfo);
  // This is expected to be called inside the Driver's Tasks's mutex.
  driver->enqueueInternal();
  if (driver->closed_) {
    return;
  }
  driver->task()->queryCtx()->executor()->add(
      [driver]() { Driver::run(driver); });
}

void Driver::init(
    std::unique_ptr<DriverCtx> ctx,
    std::vector<std::unique_ptr<Operator>> operators) {
  VELOX_CHECK_NULL(ctx_);
  ctx_ = std::move(ctx);
  enableOperatorBatchSizeStats_ =
      ctx_->queryConfig().enableOperatorBatchSizeStats();
  cpuSliceMs_ = task()->driverCpuTimeSliceLimitMs();
  VELOX_CHECK(operators_.empty());
  operators_ = std::move(operators);
  curOperatorId_ = operators_.size() - 1;
  trackOperatorCpuUsage_ = ctx_->queryConfig().operatorTrackCpuUsage();
}

void Driver::initializeOperators() {
  if (operatorsInitialized_) {
    return;
  }
  operatorsInitialized_ = true;
  for (auto& op : operators_) {
    op->initialize();
  }
}

RowVectorPtr Driver::next(
    ContinueFuture* future,
    Operator*& blockingOp,
    BlockingReason& blockingReason) {
  enqueueInternal();
  auto self = shared_from_this();
  facebook::velox::process::ScopedThreadDebugInfo scopedInfo(
      self->driverCtx()->threadDebugInfo);
  ScopedDriverThreadContext scopedDriverThreadContext(self->driverCtx());
  std::shared_ptr<BlockingState> blockingState;
  RowVectorPtr result;
  const auto stop = runInternal(self, blockingState, result);

  if (blockingState != nullptr) {
    VELOX_CHECK_NULL(result);
    *future = blockingState->future();
    blockingOp = blockingState->op();
    blockingReason = blockingState->reason();
    return nullptr;
  }

  if (stop == StopReason::kPause) {
    VELOX_CHECK_NULL(result);
    const auto paused = task()->pauseRequested(future);
    VELOX_CHECK_EQ(paused, future->valid());
    return nullptr;
  }

  // We get kBlock if 'result' was produced; kAtEnd if pipeline has finished
  // processing and no more results will be produced; kAlreadyTerminated on
  // error.
  VELOX_CHECK(
      stop == StopReason::kBlock || stop == StopReason::kAtEnd ||
      stop == StopReason::kAlreadyTerminated || stop == StopReason::kTerminate);

  return result;
}

void Driver::enqueueInternal() {
  VELOX_CHECK(!state_.isEnqueued);
  state_.isEnqueued = true;
  // When enqueuing, starting timing the queue time.
  queueTimeStartUs_ = getCurrentTimeMicro();
}

// Call an Operator method. record silenced throws, but not a query
// terminating throw. Annotate exceptions with Operator info.
#define CALL_OPERATOR(call, operatorPtr, operatorId, operatorMethod)       \
  try {                                                                    \
    Operator::NonReclaimableSectionGuard nonReclaimableGuard(operatorPtr); \
    RuntimeStatWriterScopeGuard statsWriterGuard(operatorPtr);             \
    threadNumVeloxThrow() = 0;                                             \
    opCallStatus_.start(operatorId, operatorMethod);                       \
    ExceptionContextSetter exceptionContext(                               \
        {addContextOnException, operatorPtr, true});                       \
    auto stopGuard = folly::makeGuard([&]() { opCallStatus_.stop(); });    \
    call;                                                                  \
    recordSilentThrows(*operatorPtr);                                      \
  } catch (const VeloxException&) {                                        \
    throw;                                                                 \
  } catch (const std::exception& e) {                                      \
    VELOX_FAIL(                                                            \
        "Operator::{} failed for [operator: {}, plan node ID: {}]: {}",    \
        operatorMethod,                                                    \
        operatorPtr->operatorType(),                                       \
        operatorPtr->planNodeId(),                                         \
        e.what());                                                         \
  }

void OpCallStatus::start(int32_t operatorId, const char* operatorMethod) {
  timeStartMs = getCurrentTimeMs();
  opId = operatorId;
  method = operatorMethod;
}

void OpCallStatus::stop() {
  timeStartMs = 0;
}

size_t OpCallStatusRaw::callDuration() const {
  return empty() ? 0 : (getCurrentTimeMs() - timeStartMs);
}

/*static*/ std::string OpCallStatusRaw::formatCall(
    Operator* op,
    const char* operatorMethod) {
  return op
      ? fmt::format(
            "{}.{}::{}", op->operatorType(), op->planNodeId(), operatorMethod)
      : fmt::format("null::{}", operatorMethod);
}

CpuWallTiming Driver::processLazyIoStats(
    Operator& op,
    const CpuWallTiming& timing) {
  if (&op == operators_[0].get()) {
    return timing;
  }
  auto lockStats = op.stats().wlock();

  // Checks and tries to update cpu time from lazy loads.
  auto it = lockStats->runtimeStats.find(LazyVector::kCpuNanos);
  if (it == lockStats->runtimeStats.end()) {
    // Return early if no lazy activity.  Lazy CPU and wall times are recorded
    // together, checking one is enough.
    return timing;
  }
  const int64_t cpu = it->second.sum;
  auto cpuDelta = std::max<int64_t>(0, cpu - lockStats->lastLazyCpuNanos);
  if (cpuDelta == 0) {
    // Return early if no change.  Checking one counter is enough.  If this did
    // not change and the other did, the change would be insignificant and
    // tracking would catch up when this counter next changed.
    return timing;
  }
  lockStats->lastLazyCpuNanos = cpu;

  // Checks and tries to update wall time from lazy loads.
  int64_t wallDelta = 0;
  it = lockStats->runtimeStats.find(LazyVector::kWallNanos);
  if (it != lockStats->runtimeStats.end()) {
    const int64_t wall = it->second.sum;
    wallDelta = std::max<int64_t>(0, wall - lockStats->lastLazyWallNanos);
    if (wallDelta > 0) {
      lockStats->lastLazyWallNanos = wall;
    }
  }

  // Checks and tries to update input bytes from lazy loads.
  int64_t inputBytesDelta = 0;
  it = lockStats->runtimeStats.find(LazyVector::kInputBytes);
  if (it != lockStats->runtimeStats.end()) {
    const int64_t inputBytes = it->second.sum;
    inputBytesDelta = inputBytes - lockStats->lastLazyInputBytes;
    if (inputBytesDelta > 0) {
      lockStats->lastLazyInputBytes = inputBytes;
    }
  }

  lockStats.unlock();
  cpuDelta = std::min<int64_t>(cpuDelta, timing.cpuNanos);
  wallDelta = std::min<int64_t>(wallDelta, timing.wallNanos);
  lockStats = operators_[0]->stats().wlock();
  lockStats->getOutputTiming.add(CpuWallTiming{
      1,
      static_cast<uint64_t>(wallDelta),
      static_cast<uint64_t>(cpuDelta),
  });
  lockStats->inputBytes += inputBytesDelta;
  lockStats->outputBytes += inputBytesDelta;
  return CpuWallTiming{
      1,
      timing.wallNanos - wallDelta,
      timing.cpuNanos - cpuDelta,
  };
}

bool Driver::shouldYield() const {
  if (cpuSliceMs_ == 0) {
    return false;
  }
  return execTimeMs() >= cpuSliceMs_;
}

bool Driver::checkUnderArbitration(ContinueFuture* future) {
  return task()->queryCtx()->checkUnderArbitration(future);
}

namespace {
inline void addInput(Operator* op, const RowVectorPtr& input) {
  if (FOLLY_LIKELY(!op->dryRun())) {
    op->addInput(input);
  }
}

inline void getOutput(Operator* op, RowVectorPtr& result) {
  result = op->getOutput();
  if (FOLLY_UNLIKELY(op->shouldDropOutput())) {
    result = nullptr;
  }
}
} // namespace

StopReason Driver::runInternal(
    std::shared_ptr<Driver>& self,
    std::shared_ptr<BlockingState>& blockingState,
    RowVectorPtr& result) {
  const auto now = getCurrentTimeMicro();
  const auto queuedTimeUs = now - queueTimeStartUs_;
  // Update the next operator's queueTime.
  StopReason stop =
      closed_ ? StopReason::kTerminate : task()->enter(state_, now);
  if (stop != StopReason::kNone) {
    if (stop == StopReason::kTerminate) {
      // ctx_ still has a reference to the Task. 'this' is not on
      // thread from the Task's viewpoint, hence no need to call
      // close().
      ctx_->task->setError(
          makeException("Cancelled", __FILE__, __LINE__, __FUNCTION__));
    }
    return stop;
  }

  // Update the queued time after entering the Task to ensure the stats have not
  // been deleted.
  if (curOperatorId_ < operators_.size()) {
    operators_[curOperatorId_]->addRuntimeStat(
        "queuedWallNanos",
        RuntimeCounter(queuedTimeUs * 1'000, RuntimeCounter::Unit::kNanos));
    RECORD_HISTOGRAM_METRIC_VALUE(
        kMetricDriverQueueTimeMs, queuedTimeUs / 1'000);
  }

  CancelGuard guard(self, task().get(), &state_, [&](StopReason reason) {
    // This is run on error or cancel exit.
    if (reason == StopReason::kTerminate) {
      ctx_->task->setError(
          makeException("Cancelled", __FILE__, __LINE__, __FUNCTION__));
    }
    close();
  });

  try {
    // Invoked to initialize the operators once before driver starts execution.
    initializeOperators();

    TestValue::adjust("facebook::velox::exec::Driver::runInternal", this);

    const int32_t numOperators = operators_.size();
    ContinueFuture future = ContinueFuture::makeEmpty();

    for (;;) {
      for (int32_t i = numOperators - 1; i >= 0; --i) {
        stop = task()->shouldStop();
        if (stop != StopReason::kNone) {
          guard.notThrown();
          return stop;
        }

        if (FOLLY_UNLIKELY(shouldYield())) {
          recordYieldCount();
          guard.notThrown();
          return StopReason::kYield;
        }

        auto* op = operators_[i].get();

        // In case we are blocked, this index will point to the operator, whose
        // queuedTime we should update.
        curOperatorId_ = i;

        if (FOLLY_UNLIKELY(checkUnderArbitration(&future))) {
          // Blocks the driver if the associated query is under memory
          // arbitration as it is very likely the driver run will trigger memory
          // arbitration when it needs to allocate memory, and the memory
          // arbitration will be blocked by the current running arbitration
          // until it finishes. Instead of blocking the driver thread to wait
          // for the current running arbitration, it is more efficient
          // system-wide to let driver go off thread for the other queries which
          // have free memory capacity to run during the time.
          blockingReason_ = BlockingReason::kWaitForArbitration;
          return blockDriver(self, i, std::move(future), blockingState, guard);
        }

        withDeltaCpuWallTimer(op, &OperatorStats::isBlockedTiming, [&]() {
          TestValue::adjust(
              "facebook::velox::exec::Driver::runInternal::isBlocked", op);
          CALL_OPERATOR(
              blockingReason_ = op->isBlocked(&future),
              op,
              curOperatorId_,
              kOpMethodIsBlocked);
        });
        if (blockingReason_ != BlockingReason::kNotBlocked) {
          return blockDriver(self, i, std::move(future), blockingState, guard);
        }

        if (i < numOperators - 1) {
          Operator* nextOp = operators_[i + 1].get();

          withDeltaCpuWallTimer(nextOp, &OperatorStats::isBlockedTiming, [&]() {
            CALL_OPERATOR(
                blockingReason_ = nextOp->isBlocked(&future),
                nextOp,
                curOperatorId_ + 1,
                kOpMethodIsBlocked);
          });
          if (blockingReason_ != BlockingReason::kNotBlocked) {
            return blockDriver(
                self, i + 1, std::move(future), blockingState, guard);
          }

          bool needsInput;
          CALL_OPERATOR(
              needsInput = nextOp->needsInput(),
              nextOp,
              curOperatorId_ + 1,
              kOpMethodNeedsInput);
          if (needsInput) {
            uint64_t resultBytes = 0;
            RowVectorPtr intermediateResult;
            withDeltaCpuWallTimer(op, &OperatorStats::getOutputTiming, [&]() {
              TestValue::adjust(
                  "facebook::velox::exec::Driver::runInternal::getOutput", op);
              CALL_OPERATOR(
                  getOutput(op, intermediateResult),
                  op,
                  curOperatorId_,
                  kOpMethodGetOutput);
              if (intermediateResult) {
                validateOperatorOutputResult(intermediateResult, *op);
                if (enableOperatorBatchSizeStats()) {
                  resultBytes = intermediateResult->estimateFlatSize();
                }
                auto lockedStats = op->stats().wlock();
                lockedStats->addOutputVector(
                    resultBytes, intermediateResult->size());
              }
            });
            if (intermediateResult) {
              withDeltaCpuWallTimer(
                  nextOp, &OperatorStats::addInputTiming, [&]() {
                    {
                      auto lockedStats = nextOp->stats().wlock();
                      lockedStats->addInputVector(
                          resultBytes, intermediateResult->size());
                    }
                    nextOp->traceInput(intermediateResult);
                    TestValue::adjust(
                        "facebook::velox::exec::Driver::runInternal::addInput",
                        nextOp);

                    CALL_OPERATOR(
                        addInput(nextOp, intermediateResult),
                        nextOp,
                        curOperatorId_ + 1,
                        kOpMethodAddInput);
                  });
              // The next iteration will see if operators_[i + 1] has
              // output now that it got input.
              i += 2;
              continue;
            } else {
              stop = task()->shouldStop();
              if (stop != StopReason::kNone) {
                guard.notThrown();
                return stop;
              }
              // The op is at end. If this is finishing, propagate the
              // finish to the next op. The op could have run out
              // because it is blocked. If the op is the source and it
              // is not blocked and empty, this is finished. If this is
              // not the source, just try to get output from the one
              // before.
              withDeltaCpuWallTimer(op, &OperatorStats::isBlockedTiming, [&]() {
                CALL_OPERATOR(
                    blockingReason_ = op->isBlocked(&future),
                    op,
                    curOperatorId_,
                    kOpMethodIsBlocked);
              });
              if (blockingReason_ != BlockingReason::kNotBlocked) {
                return blockDriver(
                    self, i, std::move(future), blockingState, guard);
              }

              bool finished{false};
              withDeltaCpuWallTimer(op, &OperatorStats::finishTiming, [&]() {
                CALL_OPERATOR(
                    finished = op->isFinished(),
                    op,
                    curOperatorId_,
                    kOpMethodIsFinished);
              });
              if (finished) {
                withDeltaCpuWallTimer(
                    nextOp, &OperatorStats::finishTiming, [this, &nextOp]() {
                      TestValue::adjust(
                          "facebook::velox::exec::Driver::runInternal::noMoreInput",
                          nextOp);
                      CALL_OPERATOR(
                          nextOp->noMoreInput(),
                          nextOp,
                          curOperatorId_ + 1,
                          kOpMethodNoMoreInput);
                    });
                break;
              }
            }
          }
        } else {
          // A sink (last) operator, after getting unblocked, gets
          // control here, so it can advance. If it is again blocked,
          // this will be detected when trying to add input, and we
          // will come back here after this is again on thread.
          withDeltaCpuWallTimer(op, &OperatorStats::getOutputTiming, [&]() {
            CALL_OPERATOR(
                getOutput(op, result), op, curOperatorId_, kOpMethodGetOutput);
            if (result) {
              validateOperatorOutputResult(result, *op);
              vector_size_t resultByteSize{0};
              if (enableOperatorBatchSizeStats()) {
                resultByteSize = result->estimateFlatSize();
              }
              auto lockedStats = op->stats().wlock();
              lockedStats->addOutputVector(resultByteSize, result->size());
            }
          });

          if (result) {
            // This code path is used only in serial execution mode.
            blockingReason_ = BlockingReason::kWaitForConsumer;
            guard.notThrown();
            return StopReason::kBlock;
          }

          bool finished{false};
          withDeltaCpuWallTimer(op, &OperatorStats::finishTiming, [&]() {
            CALL_OPERATOR(
                finished = op->isFinished(),
                op,
                curOperatorId_,
                kOpMethodIsFinished);
          });
          if (finished) {
            guard.notThrown();
            close();
            return StopReason::kAtEnd;
          }
          continue;
        }
      }
    }
  } catch (velox::VeloxException&) {
    task()->setError(std::current_exception());
    // The CancelPoolGuard will close 'self' and remove from Task.
    return StopReason::kAlreadyTerminated;
  } catch (std::exception&) {
    task()->setError(std::current_exception());
    // The CancelGuard will close 'self' and remove from Task.
    return StopReason::kAlreadyTerminated;
  }
}

#undef CALL_OPERATOR

// static
std::atomic_uint64_t& Driver::yieldCount() {
  static std::atomic_uint64_t count{0};
  return count;
}

// static
void Driver::recordYieldCount() {
  ++yieldCount();
  RECORD_METRIC_VALUE(kMetricDriverYieldCount);
}

// static
void Driver::run(std::shared_ptr<Driver> self) {
  process::TraceContext trace("Driver::run");
  facebook::velox::process::ScopedThreadDebugInfo scopedInfo(
      self->driverCtx()->threadDebugInfo);
  ScopedDriverThreadContext scopedDriverThreadContext(self->driverCtx());
  std::shared_ptr<BlockingState> blockingState;
  RowVectorPtr nullResult;
  auto reason = self->runInternal(self, blockingState, nullResult);

  // When Driver runs on an executor, the last operator (sink) must not produce
  // any results.
  VELOX_CHECK_NULL(
      nullResult,
      "The last operator (sink) must not produce any results. "
      "Results need to be consumed by either a callback or another operator. ");

  // There can be a race between Task terminating and the Driver being on the
  // thread and exiting the runInternal() in a blocked state. If this happens
  // the Driver won't be closed, so we need to check the Task here and exit w/o
  // going into the resume mode waiting on a promise.
  if (reason == StopReason::kBlock &&
      self->task()->shouldStop() == StopReason::kTerminate) {
    return;
  }

  switch (reason) {
    case StopReason::kBlock:
      // Set the resume action outside the Task so that, if the
      // future is already realized we do not have a second thread
      // entering the same Driver.
      BlockingState::setResume(blockingState);
      return;

    case StopReason::kYield:
      // Go to the end of the queue.
      enqueue(self);
      return;

    case StopReason::kPause:
    case StopReason::kTerminate:
    case StopReason::kAlreadyTerminated:
    case StopReason::kAtEnd:
      return;
    default:
      VELOX_FAIL("Unhandled stop reason");
  }
}

void Driver::initializeOperatorStats(std::vector<OperatorStats>& stats) {
  stats.resize(operators_.size(), OperatorStats(0, 0, "", ""));
  // Initialize the place in stats given by the operatorId. Use the
  // operatorId instead of i as the index to document the usage. The
  // operators are sequentially numbered but they could be reordered
  // in the pipeline later, so the ordinal position of the Operator is
  // not always the index into the stats.
  for (auto& op : operators_) {
    auto id = op->operatorId();
    VELOX_DCHECK_LT(id, stats.size());
    stats[id] = op->stats(false);
  }
}

void Driver::closeOperators() {
  // Close operators.
  for (auto& op : operators_) {
    op->close();
  }

  // Add operator stats to the task.
  for (auto& op : operators_) {
    auto stats = op->stats(true);
    stats.numDrivers = 1;
    task()->addOperatorStats(stats);
  }
}

void Driver::updateStats() {
  DriverStats stats;
  if (state_.totalPauseTimeMs > 0) {
    stats.runtimeStats[DriverStats::kTotalPauseTime] = RuntimeMetric(
        1'000'000 * state_.totalPauseTimeMs, RuntimeCounter::Unit::kNanos);
  }
  if (state_.totalOffThreadTimeMs > 0) {
    stats.runtimeStats[DriverStats::kTotalOffThreadTime] = RuntimeMetric(
        1'000'000 * state_.totalOffThreadTimeMs, RuntimeCounter::Unit::kNanos);
  }
  task()->addDriverStats(ctx_->pipelineId, std::move(stats));
}

void Driver::startBarrier() {
  VELOX_CHECK(ctx_->task->underBarrier());
  VELOX_CHECK(
      !barrier_.has_value(),
      "The driver has already started barrier processing");
  barrier_ = BarrierState{};
}

void Driver::drainOutput() {
  VELOX_CHECK(
      hasBarrier(), "Can't drain a driver not under barrier processing");
  VELOX_CHECK(!isDraining(), "The driver is already draining");
  // Starts to drain from the source operator.
  barrier_->drainingOpId = 0;
  drainNextOperator();
}

bool Driver::isDraining() const {
  return hasBarrier() && barrier_->drainingOpId.has_value();
}

bool Driver::isDraining(int32_t operatorId) const {
  return isDraining() && operatorId == barrier_->drainingOpId;
}

bool Driver::hasDrained(int32_t operatorId) const {
  return isDraining() && operatorId < barrier_->drainingOpId;
}

void Driver::finishDrain(int32_t operatorId) {
  VELOX_CHECK(isDraining());
  VELOX_CHECK_EQ(barrier_->drainingOpId.value(), operatorId);
  barrier_->drainingOpId = barrier_->drainingOpId.value() + 1;
  drainNextOperator();
}

void Driver::drainNextOperator() {
  VELOX_CHECK(isDraining());
  for (; barrier_->drainingOpId < operators_.size();
       barrier_->drainingOpId = barrier_->drainingOpId.value() + 1) {
    if (operators_[barrier_->drainingOpId.value()]->startDrain()) {
      break;
    }
  }
  if (barrier_->drainingOpId == operators_.size()) {
    finishBarrier();
  }
}

void Driver::dropInput(int32_t operatorId) {
  if (!hasBarrier()) {
    // No need to drop input if the driver has finished barrier processing.
    return;
  }
  VELOX_CHECK_LT(operatorId, operators_.size());
  if (!barrier_->dropInputOpId.has_value()) {
    barrier_->dropInputOpId = operatorId;
  } else {
    barrier_->dropInputOpId = std::max(*barrier_->dropInputOpId, operatorId);
  }
}

bool Driver::shouldDropOutput(int32_t operatorId) const {
  return hasBarrier() && barrier_->dropInputOpId.has_value() &&
      operatorId < *barrier_->dropInputOpId;
}

void Driver::finishBarrier() {
  VELOX_CHECK(isDraining());
  VELOX_CHECK_EQ(barrier_->drainingOpId.value(), operators_.size());
  barrier_.reset();
  ctx_->task->finishDriverBarrier();
}

void Driver::close() {
  if (closed_) {
    // Already closed.
    return;
  }
  if (!isOnThread() && !isTerminated()) {
    LOG(FATAL) << "Driver::close is only allowed from the Driver's thread";
  }
  closeOperators();
  updateStats();
  closed_ = true;
  Task::removeDriver(ctx_->task, this);
}

void Driver::closeByTask() {
  VELOX_CHECK(isOnThread());
  VELOX_CHECK(isTerminated());
  closeOperators();
  updateStats();
  closed_ = true;
}

bool Driver::mayPushdownAggregation(Operator* aggregation) const {
  for (auto i = 1; i < operators_.size(); ++i) {
    auto op = operators_[i].get();
    if (aggregation == op) {
      return true;
    }
    if (!op->isFilter() || !op->preservesOrder()) {
      return false;
    }
  }
  VELOX_FAIL(
      "Aggregation operator not found in its Driver: {}",
      aggregation->toString());
}

int Driver::operatorIndex(const Operator* op) const {
  int index = -1;
  for (auto i = 0; i < operators_.size(); ++i) {
    if (op == operators_[i].get()) {
      index = i;
      break;
    }
  }
  VELOX_CHECK_GE(
      index, 0, "Operator not found in its Driver: {}", op->toString());
  return index;
}

std::unordered_set<column_index_t> Driver::canPushdownFilters(
    const Operator* filterSource,
    const std::vector<column_index_t>& channels) const {
  const int filterSourceIndex = operatorIndex(filterSource);

  std::unordered_set<column_index_t> supportedChannels;
  for (auto i = 0; i < channels.size(); ++i) {
    auto channel = channels[i];
    for (auto j = filterSourceIndex - 1; j >= 0; --j) {
      auto* prevOp = operators_[j].get();

      if (j == 0) {
        // Source operator.
        if (prevOp->canAddDynamicFilter()) {
          supportedChannels.emplace(channels[i]);
        }
        break;
      }

      const auto& identityProjections = prevOp->identityProjections();
      const auto inputChannel =
          getIdentityProjection(identityProjections, channel);
      if (!inputChannel.has_value()) {
        // Filter channel is not an identity projection.
        if (prevOp->canAddDynamicFilter()) {
          supportedChannels.emplace(channels[i]);
        }
        break;
      }

      // Continue walking upstream.
      channel = inputChannel.value();
    }
  }

  return supportedChannels;
}

int Driver::pushdownFilters(
    Operator* filterSource,
    const std::vector<column_index_t>& channels,
    const std::function<bool(column_index_t, common::FilterPtr&)>& makeFilter) {
  const int filterSourceIndex = operatorIndex(filterSource);
  int numFiltersProduced = 0;
  std::vector<int> numFiltersAccepted(filterSourceIndex);
  for (auto i = 0; i < channels.size(); ++i) {
    auto channel = channels[i];
    int j = -1;
    for (j = filterSourceIndex - 1; j >= 0; --j) {
      auto* prevOp = operators_[j].get();
      if (j == 0) {
        // Source operator.
        break;
      }
      const auto& identityProjections = prevOp->identityProjections();
      const auto inputChannel =
          getIdentityProjection(identityProjections, channel);
      if (!inputChannel.has_value()) {
        // Filter channel is not an identity projection.
        break;
      }
      // Continue walking upstream.
      channel = inputChannel.value();
    }
    if (!(j >= 0 && operators_[j]->canAddDynamicFilter())) {
      continue;
    }
    common::FilterPtr filter;
    auto lkSource = pushdownFilters_->at(filterSourceIndex).wlock();
    if (makeFilter(i, filter)) {
      if (filter) {
        // A new filter is generated.
        auto lkTarget = pushdownFilters_->at(j).wlock();
        common::Filter::merge(filter, lkTarget->filters[channel]);
        lkTarget->dynamicFilteredColumns.insert(channel);
      } else {
        // Same filter is already generated by another operator on the same
        // node.  Just do some sanity check here.
        auto lkTarget = pushdownFilters_->at(j).rlock();
        VELOX_CHECK(
            lkTarget->filters.at(channel) &&
            lkTarget->dynamicFilteredColumns.contains(channel));
      }
      ++numFiltersProduced;
      ++numFiltersAccepted[j];
    }
  }
  for (int j = 0; j < filterSourceIndex; ++j) {
    if (numFiltersAccepted[j] == 0) {
      continue;
    }
    {
      auto lk = pushdownFilters_->at(j).rlock();
      operators_[j]->addDynamicFilterLocked(filterSource->planNodeId(), *lk);
    }
    operators_[j]->addRuntimeStat(
        "dynamicFiltersAccepted", RuntimeCounter(numFiltersAccepted[j]));
  }
  if (numFiltersProduced > 0) {
    filterSource->addRuntimeStat(
        "dynamicFiltersProduced", RuntimeCounter(numFiltersProduced));
  }
  return numFiltersProduced;
}

Operator* Driver::findOperator(std::string_view planNodeId) const {
  for (auto& op : operators_) {
    if (op->planNodeId() == planNodeId) {
      return op.get();
    }
  }
  return nullptr;
}

Operator* Driver::findOperator(int32_t operatorId) const {
  VELOX_CHECK_LT(operatorId, operators_.size());
  return operators_[operatorId].get();
}

Operator* Driver::findOperatorNoThrow(int32_t operatorId) const {
  return (operatorId < operators_.size()) ? operators_[operatorId].get()
                                          : nullptr;
}

Operator* Driver::sourceOperator() const {
  return operators_[0].get();
}

Operator* Driver::sinkOperator() const {
  return operators_[operators_.size() - 1].get();
}

std::vector<Operator*> Driver::operators() const {
  std::vector<Operator*> operators;
  operators.reserve(operators_.size());
  for (auto& op : operators_) {
    operators.push_back(op.get());
  }
  return operators;
}

std::string Driver::toString() const {
  std::stringstream out;
  out << "{Driver." << driverCtx()->pipelineId << "." << driverCtx()->driverId
      << ": ";
  if (state_.isTerminated) {
    out << "terminated, ";
  }
  if (state_.hasBlockingFuture) {
    std::string blockedOp = (blockedOperatorId_ < operators_.size())
        ? operators_[blockedOperatorId_]->toString()
        : "<unknown op>";
    out << "blocked (" << blockingReasonToString(blockingReason_) << " "
        << blockedOp << "), ";
  } else if (state_.isEnqueued) {
    out << "enqueued ";
  } else if (state_.isOnThread()) {
    out << "running ";
  } else {
    out << "unknown state";
  }

  out << "{Operators: ";
  for (auto& op : operators_) {
    out << op->toString() << ", ";
  }
  out << "}";
  const auto ocs = opCallStatus();
  if (!ocs.empty()) {
    out << "{OpCallStatus: executing "
        << ocs.formatCall(findOperatorNoThrow(ocs.opId), ocs.method) << " for "
        << ocs.callDuration() << "ms}";
  }
  out << "}";
  return out.str();
}

Driver::CancelGuard::~CancelGuard() {
  bool onTerminateCalled{false};
  if (isThrow_) {
    // Runtime error. Driver is on thread, hence safe.
    state_->isTerminated = true;
    onTerminate_(StopReason::kNone);
    onTerminateCalled = true;
  }
  task_->leave(*state_, onTerminateCalled ? nullptr : onTerminate_);
}

folly::dynamic Driver::toJson() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["blockingReason"] = blockingReasonToString(blockingReason_);
  obj["state"] = state_.toJson();
  obj["closed"] = closed_.load();
  obj["queueTimeStartMicros"] = queueTimeStartUs_;
  const auto ocs = opCallStatus();
  if (!ocs.empty()) {
    obj["curOpCall"] =
        ocs.formatCall(findOperatorNoThrow(ocs.opId), ocs.method);
    obj["curOpCallDuration"] = ocs.callDuration();
  }

  folly::dynamic operatorsObj = folly::dynamic::object;
  int index = 0;
  for (auto& op : operators_) {
    operatorsObj[std::to_string(index++)] = op->toJson();
  }
  obj["operators"] = operatorsObj;

  return obj;
}

template <typename Func>
void Driver::withDeltaCpuWallTimer(
    Operator* op,
    TimingMemberPtr opTimingMember,
    Func&& opFunction) {
  // If 'trackOperatorCpuUsage_' is true, create and initialize the timer object
  // to track cpu and wall time of the opFunction.
  if (!trackOperatorCpuUsage_) {
    opFunction();
    return;
  }

  // The delta CpuWallTiming object would be recorded to the corresponding
  // 'opTimingMember' upon destruction of the timer when withDeltaCpuWallTimer
  // ends. The timer is created on the stack to avoid heap allocation
  auto f = [op, opTimingMember, this](const CpuWallTiming& elapsedTime) {
    auto elapsedSelfTime = processLazyIoStats(*op, elapsedTime);
    op->stats().withWLock([&](auto& lockedStats) {
      (lockedStats.*opTimingMember).add(elapsedSelfTime);
    });
  };
  DeltaCpuWallTimer<decltype(f)> timer(std::move(f));

  opFunction();
}

void Driver::validateOperatorOutputResult(
    const RowVectorPtr& result,
    const Operator& op) {
  VELOX_CHECK_GT(
      result->size(),
      0,
      "Operator::getOutput() must return nullptr or a non-empty vector: {}",
      op.operatorType());

  if (ctx_->queryConfig().validateOutputFromOperators()) {
    try {
      result->validate({});
    } catch (const std::exception& e) {
      VELOX_FAIL(
          "Output validation failed for [operator: {}, plan node ID: {}]: {}",
          op.operatorType(),
          op.planNodeId(),
          e.what());
    }
  }
}

StopReason Driver::blockDriver(
    const std::shared_ptr<Driver>& self,
    size_t blockedOperatorId,
    ContinueFuture&& future,
    std::shared_ptr<BlockingState>& blockingState,
    CancelGuard& guard) {
  auto* op = operators_[blockedOperatorId].get();
  VELOX_CHECK(
      future.valid(),
      "The operator {} is blocked but blocking future is not valid",
      op->operatorType());
  VELOX_CHECK_NE(blockingReason_, BlockingReason::kNotBlocked);
  if (blockingReason_ == BlockingReason::kYield) {
    recordYieldCount();
  }
  blockedOperatorId_ = blockedOperatorId;
  blockingState = std::make_shared<BlockingState>(
      self, std::move(future), op, blockingReason_);
  guard.notThrown();
  return StopReason::kBlock;
}

std::string Driver::label() const {
  return fmt::format("<Driver {}:{}>", task()->taskId(), ctx_->driverId);
}

std::string blockingReasonToString(BlockingReason reason) {
  switch (reason) {
    case BlockingReason::kNotBlocked:
      return "kNotBlocked";
    case BlockingReason::kWaitForConsumer:
      return "kWaitForConsumer";
    case BlockingReason::kWaitForSplit:
      return "kWaitForSplit";
    case BlockingReason::kWaitForProducer:
      return "kWaitForProducer";
    case BlockingReason::kWaitForJoinBuild:
      return "kWaitForJoinBuild";
    case BlockingReason::kWaitForJoinProbe:
      return "kWaitForJoinProbe";
    case BlockingReason::kWaitForMergeJoinRightSide:
      return "kWaitForMergeJoinRightSide";
    case BlockingReason::kWaitForMemory:
      return "kWaitForMemory";
    case BlockingReason::kWaitForConnector:
      return "kWaitForConnector";
    case BlockingReason::kYield:
      return "kYield";
    case BlockingReason::kWaitForArbitration:
      return "kWaitForArbitration";
    case BlockingReason::kWaitForScanScaleUp:
      return "kWaitForScanScaleUp";
    case BlockingReason::kWaitForIndexLookup:
      return "kWaitForIndexLookup";
    default:
      VELOX_UNREACHABLE(
          fmt::format("Unknown blocking reason {}", static_cast<int>(reason)));
  }
}

DriverThreadContext* driverThreadContext() {
  return driverThreadCtx;
}

ScopedDriverThreadContext::ScopedDriverThreadContext(const DriverCtx* driverCtx)
    : savedDriverThreadCtx_(driverThreadCtx),
      currentDriverThreadCtx_(DriverThreadContext(driverCtx)) {
  driverThreadCtx = &currentDriverThreadCtx_;
}

ScopedDriverThreadContext::ScopedDriverThreadContext(
    const DriverThreadContext* _driverThreadCtx)
    : savedDriverThreadCtx_(driverThreadCtx),
      currentDriverThreadCtx_(
          _driverThreadCtx == nullptr ? nullptr
                                      : _driverThreadCtx->driverCtx()) {
  driverThreadCtx = &currentDriverThreadCtx_;
}

ScopedDriverThreadContext::~ScopedDriverThreadContext() {
  driverThreadCtx = savedDriverThreadCtx_;
}

} // namespace facebook::velox::exec
