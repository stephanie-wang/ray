#include "ray/raylet/actor_registration.h"

#include <sstream>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

ActorRegistration::ActorRegistration(const ActorTableDataT &actor_table_data)
    : actor_table_data_(actor_table_data),
      num_tasks_executed_(0) {
  if (GetActorVersion() == 0) {
    recovered_ = true;
  }
}

ActorRegistration::ActorRegistration(const ActorTableDataT &actor_table_data,
                                     const ActorCheckpointDataT &checkpoint_data)
    : actor_table_data_(actor_table_data),
      execution_dependency_(ObjectID::from_binary(checkpoint_data.execution_dependency)),
      num_tasks_executed_(0) {
  // Restore `frontier_`.
  for (size_t i = 0; i < checkpoint_data.handle_ids.size(); i++) {
    auto handle_id = ActorHandleID::from_binary(checkpoint_data.handle_ids[i]);
    auto &frontier_entry = frontier_[handle_id];
    frontier_entry.task_counter = checkpoint_data.task_counters[i];
    frontier_entry.execution_dependency =
        ObjectID::from_binary(checkpoint_data.frontier_dependencies[i]);
    RAY_LOG(DEBUG) << "Checkpoint frontier entry " << handle_id << ": " << frontier_entry.task_counter << " " << frontier_entry.execution_dependency;
    num_tasks_executed_ += frontier_entry.task_counter;
  }
  RAY_LOG(DEBUG) << "Checkpoint num tasks executed: " << num_tasks_executed_;
  // Restore `dummy_objects_`.
  for (size_t i = 0; i < checkpoint_data.unreleased_dummy_objects.size(); i++) {
    auto dummy = ObjectID::from_binary(checkpoint_data.unreleased_dummy_objects[i]);
    dummy_objects_[dummy] = checkpoint_data.num_dummy_object_dependencies[i];
  }

  for (const auto &downstream_actor_id_string : checkpoint_data.downstream_actor_ids) {
    const ActorID downstream_actor_id = ActorID::from_binary(downstream_actor_id_string);
    AddDownstreamActorId(downstream_actor_id);
  }

  recovered_ = true;
  for (const auto &upstream_actor_handle_string : checkpoint_data.upstream_actor_handle_ids) {
    const ActorHandleID upstream_actor_handle_id = ActorID::from_binary(upstream_actor_handle_string);
    // Do not include the creator of the actor. This is specific to stream
    // processing, where the driver is the original creator and does not call
    // any tasks on the actor.
    if (!upstream_actor_handle_id.is_nil()) {
      RAY_LOG(DEBUG) << "Adding handle " << upstream_actor_handle_id << " to recovery frontier";
      // We don't know what the latest task submitted by this handle was, so
      // set the counter to the max.
      recovery_frontier_[upstream_actor_handle_id] = INT64_MAX;
    }
    recovered_ = false;
  }
}

const ClientID ActorRegistration::GetNodeManagerId() const {
  return ClientID::from_binary(actor_table_data_.node_manager_id);
}

const ObjectID ActorRegistration::GetActorCreationDependency() const {
  return ObjectID::from_binary(actor_table_data_.actor_creation_dummy_object_id);
}

const ObjectID ActorRegistration::GetExecutionDependency() const {
  return execution_dependency_;
}

const DriverID ActorRegistration::GetDriverId() const {
  return DriverID::from_binary(actor_table_data_.driver_id);
}

const int64_t ActorRegistration::GetMaxReconstructions() const {
  return actor_table_data_.max_reconstructions;
}

const int64_t ActorRegistration::GetRemainingReconstructions() const {
  return actor_table_data_.remaining_reconstructions;
}

const int64_t ActorRegistration::GetActorVersion() const {
  return actor_table_data_.max_reconstructions -
         actor_table_data_.remaining_reconstructions;
}

const std::unordered_map<ActorHandleID, ActorRegistration::FrontierLeaf>
    &ActorRegistration::GetFrontier() const {
  return frontier_;
}

bool ActorRegistration::Release(const ObjectID &object_id) {
  bool release = false;
  auto it = dummy_objects_.find(object_id);
  RAY_CHECK(it != dummy_objects_.end()) << object_id;
  it->second--;
  RAY_CHECK(it->second >= 0);
  if (it->second == 0) {
    dummy_objects_.erase(it);
    release = true;
  }
  return release;
}

ObjectID ActorRegistration::ExtendFrontier(const ActorHandleID &handle_id,
                                           const ObjectID &execution_dependency) {
  auto &frontier_entry = frontier_[handle_id];
  // Release the reference to the previous cursor for this
  // actor handle, if there was one.
  ObjectID object_to_release;
  if (!frontier_entry.execution_dependency.is_nil()) {
    auto release = Release(frontier_entry.execution_dependency);
    if (release) {
      object_to_release = frontier_entry.execution_dependency;
    }
  }

  frontier_entry.task_counter++;
  frontier_entry.execution_dependency = execution_dependency;
  execution_dependency_ = execution_dependency;
  // Add the reference to the new cursor for this actor handle.
  dummy_objects_[execution_dependency]++;
  // Add a reference to the new cursor for the task that will follow this
  // task during execution.
  dummy_objects_[execution_dependency]++;

  if (!IsRecovered()) {
    auto it = recovery_frontier_.find(handle_id);
    if (it != recovery_frontier_.end()) {
      if (frontier_entry.task_counter == it->second) {
        recovery_frontier_.erase(it);
        if (recovery_frontier_.empty()) {
          RAY_LOG(DEBUG) << "RECOVERED in ExtendFrontier!";
          recovered_ = true;
        }
      }
    }
  }

  num_tasks_executed_++;
  return object_to_release;
}

void ActorRegistration::SetRecoveryFrontier(const ActorHandleID &handle_id, int64_t counter) {
  auto it = recovery_frontier_.find(handle_id);
  if (it != recovery_frontier_.end() && counter < it->second) {
    // We have seen a task that has not yet been executed and that was
    // submitted before the currently known task.
    RAY_LOG(DEBUG) << "Recovery count for handle " << handle_id << " is now " << counter;
    it->second = counter;

    if (frontier_[handle_id].task_counter == counter) {
      recovery_frontier_.erase(it);
      if (recovery_frontier_.empty()) {
        RAY_LOG(DEBUG) << "RECOVERED in SetRecoveryFrontier!";
        recovered_ = true;
      }
    }
  }
}

bool ActorRegistration::IsRecovered() const {
  return recovered_;
}

void ActorRegistration::AddHandle(const ActorHandleID &handle_id,
                                  const ObjectID &execution_dependency) {
  if (frontier_.find(handle_id) == frontier_.end()) {
    auto &new_handle = frontier_[handle_id];
    new_handle.task_counter = 0;
    new_handle.execution_dependency = execution_dependency;
    dummy_objects_[execution_dependency]++;
  }
}

int ActorRegistration::NumHandles() const { return frontier_.size(); }

std::shared_ptr<ActorCheckpointDataT> ActorRegistration::GenerateCheckpointData(
    const ActorID &actor_id, const Task &task,
    const std::vector<ActorID> &downstream_actor_ids,
    const std::vector<ActorHandleID> &upstream_actor_handle_ids) {
  const auto actor_handle_id = task.GetTaskSpecification().ActorHandleId();
  const auto dummy_object = task.GetTaskSpecification().ActorDummyObject();
  // Make a copy of the actor registration, and extend its frontier to include
  // the most recent task.
  // Note(hchen): this is needed because this method is called before
  // `FinishAssignedTask`, which will be called when the worker tries to fetch
  // the next task.
  ActorRegistration copy = *this;
  copy.ExtendFrontier(actor_handle_id, dummy_object);

  // Use actor's current state to generate checkpoint data.
  auto checkpoint_data = std::make_shared<ActorCheckpointDataT>();
  checkpoint_data->actor_id = actor_id.binary();
  checkpoint_data->execution_dependency = copy.GetExecutionDependency().binary();
  for (const auto &frontier : copy.GetFrontier()) {
    checkpoint_data->handle_ids.push_back(frontier.first.binary());
    checkpoint_data->task_counters.push_back(frontier.second.task_counter);
    checkpoint_data->frontier_dependencies.push_back(
        frontier.second.execution_dependency.binary());
  }
  for (const auto &entry : copy.GetDummyObjects()) {
    checkpoint_data->unreleased_dummy_objects.push_back(entry.first.binary());
    checkpoint_data->num_dummy_object_dependencies.push_back(entry.second);
  }
  for (const auto &downstream_actor_id : downstream_actor_ids) {
    checkpoint_data->downstream_actor_ids.push_back(downstream_actor_id.binary());
  }
  for (const auto &upstream_actor_handle_id : upstream_actor_handle_ids) {
    checkpoint_data->upstream_actor_handle_ids.push_back(upstream_actor_handle_id.binary());
  }
  return checkpoint_data;
}

void ActorRegistration::AddDownstreamActorId(const ActorID &downstream_actor_id) {
  downstream_actor_ids_.insert(downstream_actor_id);
}

bool ActorRegistration::RemoveDownstreamActorId(const ActorID &downstream_actor_id) {
  RAY_CHECK(downstream_actor_ids_.erase(downstream_actor_id) > 0);
  return downstream_actor_ids_.empty();
}

const std::unordered_set<ActorID> &ActorRegistration::GetDownstreamActorIds() {
  return downstream_actor_ids_;
}

void ActorRegistration::AddUnfinishedActorObject(const ObjectID &object_id) {
  unfinished_dummy_objects_.insert(object_id);
}

const std::unordered_set<ObjectID> ActorRegistration::GetUnfinishedActorObjects() {
  const auto unfinished_dummy_objects = std::move(unfinished_dummy_objects_);
  unfinished_dummy_objects_.clear();
  return unfinished_dummy_objects;
}

bool ActorRegistration::TaskUnfinished(const ObjectID &dummy_object) {
  return unfinished_dummy_objects_.find(dummy_object) != unfinished_dummy_objects_.end();
}

}  // namespace raylet

}  // namespace ray
