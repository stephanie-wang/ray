// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/task_manager.h"

#include "ray/util/util.h"

namespace ray {

// Start throttling task failure logs once we hit this threshold.
const int64_t kTaskFailureThrottlingThreshold = 50;

// Throttle task failure logs to once this interval.
const int64_t kTaskFailureLoggingFrequencyMillis = 5000;

void TaskManager::MaybeWriteTaskSpecToGcs(const TaskSpecification &spec) {
  // Use gcs_client_ as feature flag.
  if (gcs_client_) {
    std::shared_ptr<gcs::TaskTableData> data = std::make_shared<gcs::TaskTableData>();
    data->mutable_task()->mutable_task_spec()->CopyFrom(spec.GetMessage());
    RAY_CHECK_OK(gcs_client_->Tasks().SyncAdd(data));
  }
}

void TaskManager::MaybeIncrementGcsRefcounts(const std::vector<ObjectID> &object_ids) {
  // Use gcs_client_ as feature flag.
  if (gcs_client_) {
    for (const ObjectID &object_id : object_ids) {
      RAY_LOG(DEBUG) << "REDIS: INCR " << object_id.Hex();
      int64_t new_val =
          gcs_client_->primary_context()->IncrDecrSync("INCR %b", object_id.Hex());
      RAY_LOG(DEBUG) << "New val: " << new_val;
    }
  }
}

void TaskManager::MaybeDecrementGcsRefcounts(const std::vector<ObjectID> &object_ids) {
  {
    absl::MutexLock lock(&mu_);
    // Use gcs_client_ as feature flag.
    if (gcs_client_) {
      for (const ObjectID &object_id : object_ids) {
        RAY_LOG(DEBUG) << "REDIS: DECR " << object_id.Hex();
        int64_t new_val =
            gcs_client_->primary_context()->IncrDecrSync("DECR %b", object_id.Hex());
        RAY_LOG(DEBUG) << "New val: " << new_val;
      }
    }
  }
}

void TaskManager::AddPendingTask(const TaskID &caller_id,
                                 const rpc::Address &caller_address,
                                 const TaskSpecification &spec, int max_retries) {
  RAY_LOG(DEBUG) << "Adding pending task " << spec.TaskId();
  absl::MutexLock lock(&mu_);
  std::pair<TaskSpecification, int> entry = {spec, max_retries};
  RAY_CHECK(pending_tasks_.emplace(spec.TaskId(), std::move(entry)).second);

  // Add references for the dependencies to the task.
  std::vector<ObjectID> task_deps;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      for (size_t j = 0; j < spec.ArgIdCount(i); j++) {
        task_deps.push_back(spec.ArgId(i, j));
        RAY_LOG(DEBUG) << "Adding arg ID " << spec.ArgId(i, j);
      }
    } else {
      const auto &inlined_ids = spec.ArgInlinedIds(i);
      for (const auto &inlined_id : inlined_ids) {
        task_deps.push_back(inlined_id);
        RAY_LOG(DEBUG) << "Adding inlined ID " << inlined_id;
      }
    }
  }
  if (spec.IsActorTask()) {
    const auto actor_creation_return_id =
        spec.ActorCreationDummyObjectId().WithTransportType(TaskTransportType::DIRECT);
    task_deps.push_back(actor_creation_return_id);
  }
  reference_counter_->UpdateSubmittedTaskReferences(task_deps);

  // Add new owned objects for the return values of the task.
  size_t num_returns = spec.NumReturns();
  if (spec.IsActorTask()) {
    num_returns--;
  }
  for (size_t i = 0; i < num_returns; i++) {
    // We pass an empty vector for inner IDs because we do not know the return
    // value of the task yet. If the task returns an ID(s), the worker will
    // notify us via the WaitForRefRemoved RPC that we are now a borrower for
    // the inner IDs. Note that this RPC can be received *before* the
    // PushTaskReply.
    reference_counter_->AddOwnedObject(spec.ReturnId(i, TaskTransportType::DIRECT),
                                       /*inner_ids=*/{}, caller_id, caller_address);
  }

  // XXX: Centralized.
  MaybeWriteTaskSpecToGcs(spec);
  MaybeIncrementGcsRefcounts(task_deps);
}

void TaskManager::DrainAndShutdown(std::function<void()> shutdown) {
  absl::MutexLock lock(&mu_);
  if (pending_tasks_.empty()) {
    shutdown();
  } else {
    RAY_LOG(WARNING)
        << "This worker is still managing " << pending_tasks_.size()
        << " in flight tasks, waiting for them to finish before shutting down.";
  }
  shutdown_hook_ = shutdown;
}

bool TaskManager::IsTaskPending(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  return pending_tasks_.count(task_id) > 0;
}

void TaskManager::CompletePendingTask(const TaskID &task_id,
                                      const rpc::PushTaskReply &reply,
                                      const rpc::Address &worker_addr) {
  RAY_LOG(DEBUG) << "Completing task " << task_id;
  TaskSpecification spec;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_tasks_.find(task_id);
    RAY_CHECK(it != pending_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    spec = it->second.first;
    pending_tasks_.erase(it);

    // XXX: Centralized.
    MaybeWriteTaskSpecToGcs(spec);
  }

  RemoveFinishedTaskReferences(spec, worker_addr, reply.borrowed_refs());

  for (int i = 0; i < reply.return_objects_size(); i++) {
    const auto &return_object = reply.return_objects(i);
    ObjectID object_id = ObjectID::FromBinary(return_object.object_id());

    if (return_object.in_plasma()) {
      // Mark it as in plasma with a dummy object.
      RAY_CHECK_OK(
          in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
    } else {
      std::shared_ptr<LocalMemoryBuffer> data_buffer;
      if (return_object.data().size() > 0) {
        data_buffer = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(
                reinterpret_cast<const uint8_t *>(return_object.data().data())),
            return_object.data().size());
      }
      std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
      if (return_object.metadata().size() > 0) {
        metadata_buffer = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(
                reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
            return_object.metadata().size());
      }
      RAY_CHECK_OK(in_memory_store_->Put(
          RayObject(data_buffer, metadata_buffer,
                    IdVectorFromProtobuf<ObjectID>(return_object.nested_inlined_ids())),
          object_id));
    }
  }

  ShutdownIfNeeded();
}

void TaskManager::PendingTaskFailed(const TaskID &task_id, rpc::ErrorType error_type,
                                    Status *status) {
  // Note that this might be the __ray_terminate__ task, so we don't log
  // loudly with ERROR here.
  RAY_LOG(DEBUG) << "Task " << task_id << " failed with error "
                 << rpc::ErrorType_Name(error_type);
  int num_retries_left = 0;
  TaskSpecification spec;
  {
    absl::MutexLock lock(&mu_);
    auto it = pending_tasks_.find(task_id);
    RAY_CHECK(it != pending_tasks_.end())
        << "Tried to complete task that was not pending " << task_id;
    spec = it->second.first;
    num_retries_left = it->second.second;
    if (num_retries_left == 0) {
      pending_tasks_.erase(it);
    } else {
      RAY_CHECK(num_retries_left > 0);
      it->second.second--;
    }

    // XXX: Centralized.
    MaybeWriteTaskSpecToGcs(spec);
  }

  // We should not hold the lock during these calls because they may trigger
  // callbacks in this or other classes.
  if (num_retries_left > 0) {
    RAY_LOG(ERROR) << num_retries_left << " retries left for task " << spec.TaskId()
                   << ", attempting to resubmit.";
    retry_task_callback_(spec);
  } else {
    // Throttled logging of task failure errors.
    {
      absl::MutexLock lock(&mu_);
      auto debug_str = spec.DebugString();
      if (debug_str.find("__ray_terminate__") == std::string::npos &&
          (num_failure_logs_ < kTaskFailureThrottlingThreshold ||
           (current_time_ms() - last_log_time_ms_) >
               kTaskFailureLoggingFrequencyMillis)) {
        if (num_failure_logs_++ == kTaskFailureThrottlingThreshold) {
          RAY_LOG(ERROR) << "Too many failure logs, throttling to once every "
                         << kTaskFailureLoggingFrequencyMillis << " millis.";
        }
        last_log_time_ms_ = current_time_ms();
        if (status != nullptr) {
          RAY_LOG(ERROR) << "Task failed: " << *status << ": " << spec.DebugString();
        } else {
          RAY_LOG(ERROR) << "Task failed: " << spec.DebugString();
        }
      }
    }
    // The worker failed to execute the task, so it cannot be borrowing any
    // objects.
    RemoveFinishedTaskReferences(spec, rpc::Address(),
                                 ReferenceCounter::ReferenceTableProto());
    MarkPendingTaskFailed(task_id, spec, error_type);
  }

  ShutdownIfNeeded();
}

void TaskManager::ShutdownIfNeeded() {
  absl::MutexLock lock(&mu_);
  if (shutdown_hook_ && pending_tasks_.empty()) {
    RAY_LOG(WARNING) << "All in flight tasks finished, shutting down worker.";
    shutdown_hook_();
  }
}

void TaskManager::OnTaskDependenciesInlined(
    const std::vector<ObjectID> &inlined_dependency_ids,
    const std::vector<ObjectID> &contained_ids) {
  std::vector<ObjectID> deleted;
  reference_counter_->UpdateSubmittedTaskReferences(
      /*argument_ids_to_add=*/contained_ids,
      /*argument_ids_to_remove=*/inlined_dependency_ids, &deleted);
  in_memory_store_->Delete(deleted);
}

void TaskManager::RemoveFinishedTaskReferences(
    TaskSpecification &spec, const rpc::Address &borrower_addr,
    const ReferenceCounter::ReferenceTableProto &borrowed_refs) {
  std::vector<ObjectID> plasma_dependencies;
  for (size_t i = 0; i < spec.NumArgs(); i++) {
    if (spec.ArgByRef(i)) {
      for (size_t j = 0; j < spec.ArgIdCount(i); j++) {
        plasma_dependencies.push_back(spec.ArgId(i, j));
      }
    } else {
      const auto &inlined_ids = spec.ArgInlinedIds(i);
      plasma_dependencies.insert(plasma_dependencies.end(), inlined_ids.begin(),
                                 inlined_ids.end());
    }
  }
  if (spec.IsActorTask()) {
    const auto actor_creation_return_id =
        spec.ActorCreationDummyObjectId().WithTransportType(TaskTransportType::DIRECT);
    plasma_dependencies.push_back(actor_creation_return_id);
  }

  std::vector<ObjectID> deleted;
  reference_counter_->UpdateFinishedTaskReferences(plasma_dependencies, borrower_addr,
                                                   borrowed_refs, &deleted);
  in_memory_store_->Delete(deleted);

  // XXX: Centralized.
  MaybeDecrementGcsRefcounts(plasma_dependencies);
}

void TaskManager::MarkPendingTaskFailed(const TaskID &task_id,
                                        const TaskSpecification &spec,
                                        rpc::ErrorType error_type) {
  RAY_LOG(DEBUG) << "Treat task as failed. task_id: " << task_id
                 << ", error_type: " << ErrorType_Name(error_type);
  int64_t num_returns = spec.NumReturns();
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::ForTaskReturn(
        task_id, /*index=*/i + 1,
        /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT));
    RAY_CHECK_OK(in_memory_store_->Put(RayObject(error_type), object_id));
  }

  if (spec.IsActorCreationTask()) {
    // Publish actor death if actor creation task failed after
    // a number of retries.
    actor_manager_->PublishTerminatedActor(spec);
  }
}

TaskSpecification TaskManager::GetTaskSpec(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = pending_tasks_.find(task_id);
  RAY_CHECK(it != pending_tasks_.end());
  return it->second.first;
}

}  // namespace ray
