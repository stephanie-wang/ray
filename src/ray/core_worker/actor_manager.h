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

#ifndef RAY_CORE_WORKER_ACTOR_MANAGER_H
#define RAY_CORE_WORKER_ACTOR_MANAGER_H

#include "ray/core_worker/actor_handle.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

// Interface for testing.
class ActorManagerInterface {
 public:
  virtual void PublishTerminatedActor(const TaskSpecification &actor_creation_task) = 0;

  virtual void IncrementCompletedTasks(const ActorID &actor_id,
                                       const TaskID &caller_id) = 0;

  virtual ~ActorManagerInterface() {}
};

/// Class to manage lifetimes of actors that we create (actor children).
/// Currently this class is only used to publish actor DEAD event
/// for actor creation task failures. All other cases are managed
/// by raylet.
class ActorManager : public ActorManagerInterface {
 public:
  ActorManager(gcs::ActorInfoAccessor &actor_accessor)
      : actor_accessor_(actor_accessor) {}

  /// Called when an actor that we own can no longer be restarted.
  void PublishTerminatedActor(const TaskSpecification &actor_creation_task) override;

  const std::shared_ptr<ActorHandle> &GetHandle(const ActorID &actor_id) const;

  bool AddHandle(const ActorID &actor_id, std::shared_ptr<ActorHandle> actor_handle);

  void RemoveHandle(const ActorID &actor_id);

  void SetActorTaskSpec(const ActorID &actor_id, TaskSpecBuilder &builder, const ObjectID &new_cursor);

  void IncrementCompletedTasks(const ActorID &actor_id, const TaskID &caller_id) override;

  void ResetAllCallerState();

  const std::vector<TaskSpecification> HandleActorAlive(const ActorID &actor_id);
  void HandleActorReconstructing(const ActorID &actor_id);
  const std::vector<TaskSpecification> HandleActorDead(const ActorID &actor_id);

  void SubmitTask(TaskSpecification &spec, bool *submit, bool *cancel);

  std::vector<ActorID> ActorIds() const;

 private:
  mutable absl::Mutex mutex_;

  /// Global database of actors.
  gcs::ActorInfoAccessor &actor_accessor_;

  /// Map from actor ID to a handle to that actor.
  absl::flat_hash_map<ActorID, std::shared_ptr<ActorHandle>> actor_handles_
      GUARDED_BY(mutex_);

  std::unordered_map<ActorID, std::vector<TaskSpecification>> actor_tasks_to_resubmit_
      GUARDED_BY(mutex_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_ACTOR_MANAGER_H
