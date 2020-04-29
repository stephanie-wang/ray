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

#include "ray/core_worker/actor_manager.h"
#include "ray/gcs/pb_util.h"
#include "ray/gcs/redis_accessor.h"

namespace ray {

void ActorManager::PublishTerminatedActor(const TaskSpecification &actor_creation_task) {
  auto actor_id = actor_creation_task.ActorCreationId();
  auto data = gcs::CreateActorTableData(actor_creation_task, rpc::Address(),
                                        rpc::ActorTableData::DEAD, 0);

  auto update_callback = [actor_id](Status status) {
    if (!status.ok()) {
      // Only one node at a time should succeed at creating or updating the actor.
      RAY_LOG(ERROR) << "Failed to update state to DEAD for actor " << actor_id
                     << ", error: " << status.ToString();
    }
  };
  RAY_CHECK_OK(actor_accessor_.AsyncRegister(data, update_callback));
}

const std::shared_ptr<ActorHandle> &ActorManager::GetHandle(
    const ActorID &actor_id) const {
  absl::MutexLock lock(&mutex_);
  static const std::shared_ptr<ActorHandle> empty_ptr = nullptr;
  auto it = actor_handles_.find(actor_id);
  if (it == actor_handles_.end()) {
    return empty_ptr;
  }
  return it->second;
}

bool ActorManager::AddHandle(const ActorID &actor_id,
                             std::shared_ptr<ActorHandle> actor_handle) {
  absl::MutexLock lock(&mutex_);
  return actor_handles_.emplace(actor_id, std::move(actor_handle)).second;
}

void ActorManager::IncrementCompletedTasks(const ActorID &actor_id,
                                           const TaskID &caller_id) {
  absl::MutexLock lock(&mutex_);
  auto handle = actor_handles_.find(actor_id);
  RAY_CHECK(handle != actor_handles_.end());
  if (handle->second->RestartOption() ==
      rpc::ActorHandle::RESTART_AND_RETRY_FAILED_TASKS) {
    // Increment the number of completed tasks if failed tasks can be retried
    // on this actor. This is to make sure that if the actor restarts, we
    // resubmit failed tasks with the correct offset.
    handle->second->IncrementCompletedTasks(caller_id);
  }
}

void ActorManager::RemoveHandle(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  RAY_CHECK(actor_handles_.erase(actor_id));
}

void ActorManager::ResetAllCallerState() {
  absl::MutexLock lock(&mutex_);
  // Reset the task counters to 0 for the next caller.
  for (const auto &handle : actor_handles_) {
    handle.second->ResetCallerState();
  }
}

void ActorManager::SetActorTaskSpec(const ActorID &actor_id, TaskSpecification &spec) {
  absl::MutexLock lock(&mutex_);
  auto handle = actor_handles_.find(actor_id);
  RAY_CHECK(handle != actor_handles_.end());
  handle->second->SetActorTaskSpec(actor_id, spec);
}

const std::vector<TaskSpecification> ActorManager::HandleActorAlive(
    const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  auto handle = actor_handles_.find(actor_id);
  RAY_CHECK(handle != actor_handles_.end());
  handle->second->SetState(gcs::ActorTableData::ALIVE);
  // We have to reset the actor handle since the next instance of the
  // actor will not have the last sequence number that we sent.
  // TODO(swang): This assumes that all task replies from the previous
  // incarnation of the actor have been received.
  handle->second->ResetCallersStartAt();

  std::vector<TaskSpecification> tasks;
  const auto queue = actor_tasks_to_resubmit_.find(actor_id);
  if (queue != actor_tasks_to_resubmit_.end()) {
    tasks = std::move(queue->second);
    actor_tasks_to_resubmit_.erase(queue);
  }

  for (auto &task : tasks) {
    auto counter = task.ActorCounter();
    handle->second->SetActorCounterStartsAt(task);
    RAY_LOG(DEBUG) << "Task counter was " << counter << " now is " << task.ActorCounter();
  }
  return tasks;
}

void ActorManager::HandleActorReconstructing(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  auto handle = actor_handles_.find(actor_id);
  RAY_CHECK(handle != actor_handles_.end());
  handle->second->SetState(gcs::ActorTableData::RECONSTRUCTING);
  if (handle->second->RestartOption() == rpc::ActorHandle::RESTART) {
    // Reset the caller state so that the next task that gets submitted will
    // start off with count 0.
    handle->second->ResetCallerState();
  }
}

const std::vector<TaskSpecification> ActorManager::HandleActorDead(
    const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  auto handle = actor_handles_.find(actor_id);
  RAY_CHECK(handle != actor_handles_.end());
  handle->second->SetState(gcs::ActorTableData::DEAD);

  std::vector<TaskSpecification> tasks;
  const auto queue = actor_tasks_to_resubmit_.find(actor_id);
  if (queue != actor_tasks_to_resubmit_.end()) {
    tasks = std::move(queue->second);
    actor_tasks_to_resubmit_.erase(queue);
  }
  return tasks;

  // We cannot erase the actor handle here because clients can still
  // submit tasks to dead actors. This also means we defer unsubscription,
  // otherwise we crash when bulk unsubscribing all actor handles.
}

void ActorManager::SubmitTask(TaskSpecification &spec, bool *submit, bool *cancel) {
  absl::MutexLock lock(&mutex_);
  auto handle = actor_handles_.find(spec.ActorId());
  RAY_CHECK(handle != actor_handles_.end());

  const auto state = handle->second->GetState();
  if (state == gcs::ActorTableData::ALIVE) {
    *submit = true;
    if (handle->second->RestartOption() ==
        rpc::ActorHandle::RESTART_AND_RETRY_FAILED_TASKS) {
      handle->second->SetActorCounterStartsAt(spec);
    }
  } else if (state == gcs::ActorTableData::DEAD) {
    *cancel = true;
  } else {
    RAY_LOG(DEBUG) << "Queueing actor task for resubmission " << spec.TaskId();
    actor_tasks_to_resubmit_[spec.ActorId()].push_back(spec);
  }
}

std::vector<ActorID> ActorManager::ActorIds() const {
  absl::MutexLock lock(&mutex_);
  std::vector<ActorID> ids;
  for (const auto &actor : actor_handles_) {
    ids.push_back(actor.first);
  }
  return ids;
}

}  // namespace ray
