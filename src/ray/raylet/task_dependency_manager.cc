#include "task_dependency_manager.h"

namespace ray {

namespace raylet {

TaskDependencyManager::TaskDependencyManager(
    ObjectManagerInterface &object_manager,
    ReconstructionPolicyInterface &reconstruction_policy,
    boost::asio::io_service &io_service, const ClientID &client_id,
    int64_t initial_lease_period_ms,
    gcs::TableInterface<TaskID, TaskLeaseData> &task_lease_table)
    : object_manager_(object_manager),
      reconstruction_policy_(reconstruction_policy),
      io_service_(io_service),
      client_id_(client_id),
      initial_lease_period_ms_(initial_lease_period_ms),
      task_lease_table_(task_lease_table) {}

bool TaskDependencyManager::CheckObjectLocal(const ObjectID &object_id) const {
  return local_objects_.count(object_id) == 1;
}

bool TaskDependencyManager::CheckObjectRequired(const ObjectID &object_id,
                                                bool *fast_reconstruction,
                                                bool *delay_pull) const {
  const TaskID task_id = ComputeTaskId(object_id);
  auto task_entry = required_tasks_.find(task_id);
  // If there are no subscribed tasks that are dependent on the object, then do
  // nothing.
  if (task_entry == required_tasks_.end()) {
    return false;
  }
  auto it = task_entry->second.required_objects.find(object_id);
  if (it == task_entry->second.required_objects.end()) {
    return false;
  }
  // If the object is already local, then the dependency is fulfilled. Do
  // nothing.
  if (local_objects_.count(object_id) == 1) {
    return false;
  }
  // If the task that creates the object is pending execution, then the
  // dependency will be fulfilled locally. Do nothing.
  if (pending_tasks_.count(task_id) == 1) {
    return false;
  }
  if (fast_reconstruction) {
    *fast_reconstruction = task_entry->second.fast_reconstruction;
  }
  if (delay_pull) {
    *delay_pull = it->second.second;
  }
  return true;
}

void TaskDependencyManager::HandleRemoteDependencyRequired(const ObjectID &object_id) {
  bool fast_reconstruction = false;
  bool delay_pull = false;
  bool required = CheckObjectRequired(object_id, &fast_reconstruction, &delay_pull);
  // If the object is required, then try to make the object available locally.
  if (required) {
    auto inserted = required_objects_.insert(object_id);
    if (inserted.second) {
      // If we haven't already, request the object manager to pull it from a
      // remote node.
      RAY_CHECK_OK(object_manager_.Pull(object_id, delay_pull));
      reconstruction_policy_.ListenAndMaybeReconstruct(object_id, fast_reconstruction);
    }
  }
}

void TaskDependencyManager::HandleRemoteDependencyCanceled(const ObjectID &object_id) {
  bool required = CheckObjectRequired(object_id, nullptr, nullptr);
  // If the object is no longer required, then cancel the object.
  if (!required) {
    auto it = required_objects_.find(object_id);
    if (it != required_objects_.end()) {
      object_manager_.CancelPull(object_id);
      reconstruction_policy_.Cancel(object_id);
      required_objects_.erase(it);
    }
  }
}

std::vector<TaskID> TaskDependencyManager::HandleObjectLocal(
    const ray::ObjectID &object_id) {
  // Add the object to the table of locally available objects.
  auto inserted = local_objects_.insert(object_id);
  RAY_CHECK(inserted.second);

  // Find any tasks that are dependent on the newly available object.
  std::vector<TaskID> ready_task_ids;
  std::vector<TaskID> task_ids_to_remove;
  auto creating_task_entry = required_tasks_.find(ComputeTaskId(object_id));
  if (creating_task_entry != required_tasks_.end()) {
    auto &required_objects = creating_task_entry->second.required_objects;
    auto object_entry = required_objects.find(object_id);
    if (object_entry != required_objects.end()) {
      for (auto &dependent_task_id : object_entry->second.first) {
        auto it = task_dependencies_.find(dependent_task_id);
        RAY_CHECK(it != task_dependencies_.end());
        auto &task_entry = it->second;

        // If the task called ray.get on the object, then the task may now be
        // ready to run.
        if (task_entry.get_dependencies.find(object_id) !=
            task_entry.get_dependencies.end()) {
          task_entry.num_missing_dependencies--;
          // If the dependent task now has all of its arguments ready, it's
          // ready to run.
          if (task_entry.num_missing_dependencies == 0) {
            ready_task_ids.push_back(dependent_task_id);
          }
          // ray.get dependencies stay active until UnsubscribeDependencies is
          // called, so we do not remove the dependency here.
        }

        // If the task called ray.wait on the object, then we can now remove
        // the task's dependency on the object.
        auto wait_it = task_entry.wait_dependencies.find(object_id);
        if (wait_it != task_entry.wait_dependencies.end()) {
          // The object is now local, so the ray.wait call has been fulfilled.
          task_entry.wait_dependencies.erase(wait_it);
          if (task_entry.get_dependencies.find(object_id) ==
              task_entry.get_dependencies.end()) {
            // The task called ray.wait on the object, but not ray.get, so it
            // no longer depends on the object. Therefore, it is safe to remove
            // the dependent task from the set of tasks that depend on the
            // local object.
            task_ids_to_remove.push_back(dependent_task_id);
          }
        }

        // All ray.get and ray.wait dependencies have been fulfilled for this
        // task, so it is safe to remove.
        if (task_entry.get_dependencies.empty() && task_entry.wait_dependencies.empty()) {
          task_dependencies_.erase(it);
        }
      }
    }
  }

  // Clear the tasks that no longer depend on the local object.
  for (const auto &dependent_task_id : task_ids_to_remove) {
    RemoveTaskDependency(dependent_task_id, object_id);
  }

  // The object is now local, so cancel any in-progress operations to make the
  // object local.
  HandleRemoteDependencyCanceled(object_id);

  return ready_task_ids;
}

void TaskDependencyManager::HandleTaskResubmitted(
    const ray::TaskID &task_id) {
  auto creating_task_entry = required_tasks_.find(task_id);
  if (creating_task_entry == required_tasks_.end()) {
    return;
  }

  auto &required_objects = creating_task_entry->second.required_objects;
  for (auto required_object_it = required_objects.begin(); required_object_it
      != required_objects.end(); ) {
    const auto object_id = required_object_it->first;
    // Find any tasks that are dependent on the newly available object.
    std::vector<TaskID> task_ids_to_remove;
    for (auto &dependent_task_id : required_object_it->second.first) {
      auto it = task_dependencies_.find(dependent_task_id);
      RAY_CHECK(it != task_dependencies_.end());
      auto &task_entry = it->second;

      // If the task called ray.wait on the object, then we can now remove
      // the task's dependency on the object.
      auto wait_it = task_entry.wait_dependencies.find(object_id);
      if (wait_it != task_entry.wait_dependencies.end()) {
        RAY_LOG(DEBUG) << "Removing wait dependency " << dependent_task_id << " on " << object_id;
        // The object is now local, so the ray.wait call has been fulfilled.
        task_entry.wait_dependencies.erase(wait_it);
        if (task_entry.get_dependencies.find(object_id) ==
            task_entry.get_dependencies.end()) {
          // The task called ray.wait on the object, but not ray.get, so it
          // no longer depends on the object. Therefore, it is safe to remove
          // the dependent task from the set of tasks that depend on the
          // local object.
          task_ids_to_remove.push_back(dependent_task_id);
        }
      }

      // All ray.get and ray.wait dependencies have been fulfilled for this
      // task, so it is safe to remove.
      if (task_entry.get_dependencies.empty() && task_entry.wait_dependencies.empty()) {
        task_dependencies_.erase(it);
      }
    }

    for (const auto &task_id : task_ids_to_remove) {
      required_object_it->second.first.erase(task_id);
    }

    if (required_object_it->second.first.empty()) {
      required_object_it = required_objects.erase(required_object_it);
    } else {
      required_object_it++;
    }

    // The object is now local, so cancel any in-progress operations to make the
    // object local.
    HandleRemoteDependencyCanceled(object_id);
   }
}

std::vector<TaskID> TaskDependencyManager::HandleObjectMissing(
    const ray::ObjectID &object_id) {
  // Remove the object from the table of locally available objects.
  auto erased = local_objects_.erase(object_id);
  RAY_CHECK(erased == 1);

  // Find any tasks that are dependent on the missing object.
  std::vector<TaskID> waiting_task_ids;
  const TaskID creating_task_id = ComputeTaskId(object_id);
  auto creating_task_entry = required_tasks_.find(creating_task_id);
  if (creating_task_entry != required_tasks_.end()) {
    auto &required_objects = creating_task_entry->second.required_objects;
    auto object_entry = required_objects.find(object_id);
    if (object_entry != required_objects.end()) {
      for (auto &dependent_task_id : object_entry->second.first) {
        auto &task_entry = task_dependencies_[dependent_task_id];
        // ray.wait dependencies are removed as soon as an object becomes
        // local, so check that the object was a ray.get dependency.
        RAY_CHECK(task_entry.get_dependencies.find(object_id) !=
                  task_entry.get_dependencies.end());
        RAY_CHECK(task_entry.wait_dependencies.find(object_id) ==
                  task_entry.wait_dependencies.end());
        // If the dependent task had all of its arguments ready, it was ready to
        // run but must be switched to waiting since one of its arguments is now
        // missing.
        if (task_entry.num_missing_dependencies == 0) {
          waiting_task_ids.push_back(dependent_task_id);
          // During normal execution we should be able to include the check
          // RAY_CHECK(pending_tasks_.count(dependent_task_id) == 1);
          // However, this invariant will not hold during unit test execution.
        }
        task_entry.num_missing_dependencies++;
      }
    }
  }
  // The object is no longer local. Try to make the object local if necessary.
  HandleRemoteDependencyRequired(object_id);
  // Process callbacks for all of the tasks dependent on the object that are
  // now ready to run.
  return waiting_task_ids;
}

bool TaskDependencyManager::SubscribeDependencies(
    const TaskID &task_id, const std::vector<ObjectID> &required_objects,
    bool ray_get,
    bool delay_pull,
    bool fast_reconstruction) {
  auto &task_entry = task_dependencies_[task_id];

  // Record the task's dependencies.
  for (const auto &object_id : required_objects) {
    auto inserted = (ray_get) ? task_entry.get_dependencies.insert(object_id)
                              : task_entry.wait_dependencies.insert(object_id);
    if (inserted.second) {
      // Get the ID of the task that creates the dependency.
      TaskID creating_task_id = ComputeTaskId(object_id);
      // Determine whether the dependency can be fulfilled by the local node.
      if (ray_get && local_objects_.count(object_id) == 0) {
        // The object is not local.
        task_entry.num_missing_dependencies++;
      }
      // Add the subscribed task to the mapping from object ID to list of
      // dependent tasks.
      auto &required_task = required_tasks_[creating_task_id];
      required_task.required_objects[object_id].first.insert(task_id);
      if (delay_pull) {
        required_task.required_objects[object_id].second = true;
      }
      if (fast_reconstruction) {
        required_task.fast_reconstruction = true;
      }
    }
  }

  // These dependencies are required by the given task. Try to make them local
  // if necessary.
  for (const auto &object_id : required_objects) {
    HandleRemoteDependencyRequired(object_id);
  }

  // Return whether all dependencies are local.
  return (task_entry.num_missing_dependencies == 0);
}

void TaskDependencyManager::RemoveTaskDependency(const TaskID &dependent_task_id,
                                                 const ObjectID &object_id) {
  // Remove the task from the list of tasks that are dependent on this
  // object.
  // Get the ID of the task that creates the dependency.
  const TaskID creating_task_id = ComputeTaskId(object_id);
  auto creating_task_entry = required_tasks_.find(creating_task_id);
  auto &dependent_tasks = creating_task_entry->second.required_objects[object_id];
  size_t erased = dependent_tasks.first.erase(dependent_task_id);
  RAY_CHECK(erased > 0);
  // If the unsubscribed task was the only task dependent on the object, then
  // erase the object entry.
  if (dependent_tasks.first.empty()) {
    creating_task_entry->second.required_objects.erase(object_id);
    // Remove the task that creates this object if there are no more object
    // dependencies created by the task.
    if (creating_task_entry->second.required_objects.empty()) {
      required_tasks_.erase(creating_task_entry);
    }
  }
}

bool TaskDependencyManager::UnsubscribeGetDependencies(const TaskID &task_id) {
  // Remove the task from the table of subscribed tasks.
  auto it = task_dependencies_.find(task_id);
  if (it == task_dependencies_.end()) {
    return false;
  }

  // Update the task to reflect that it is no longer blocked on any objects.
  // It may still have active ray.wait calls, so we cannot remove it
  // immediately.
  TaskDependencies &task_entry = it->second;
  const auto get_dependencies = std::move(task_entry.get_dependencies);
  task_entry.get_dependencies.clear();
  task_entry.num_missing_dependencies = 0;
  // If the task does not have any active ray.wait calls, then it is safe to
  // remove it completely.
  if (task_entry.wait_dependencies.empty()) {
    task_dependencies_.erase(it);
  }

  // Remove the task's dependencies.
  for (const auto &object_id : get_dependencies) {
    RemoveTaskDependency(task_id, object_id);
  }
  // These dependencies are no longer required by the given task. Try to cancel
  // any in-progress operations to make them local.
  for (const auto &object_id : get_dependencies) {
    HandleRemoteDependencyCanceled(object_id);
  }

  return true;
}

bool TaskDependencyManager::UnsubscribeAllDependencies(const TaskID &task_id) {
  // Remove the task from the table of subscribed tasks.
  auto it = task_dependencies_.find(task_id);
  if (it == task_dependencies_.end()) {
    return false;
  }

  const TaskDependencies task_entry = std::move(it->second);
  task_dependencies_.erase(it);

  // Remove the task's dependencies.
  for (const auto &object_id : task_entry.get_dependencies) {
    RemoveTaskDependency(task_id, object_id);
  }
  for (const auto &object_id : task_entry.wait_dependencies) {
    RemoveTaskDependency(task_id, object_id);
  }

  // These dependencies are no longer required by the given task. Cancel any
  // in-progress operations to make them local.
  for (const auto &object_id : task_entry.get_dependencies) {
    HandleRemoteDependencyCanceled(object_id);
  }
  for (const auto &object_id : task_entry.wait_dependencies) {
    HandleRemoteDependencyCanceled(object_id);
  }

  return true;
}

bool TaskDependencyManager::UnsubscribeWaitDependency(const TaskID &task_id,
                                                      const ObjectID &object_id) {
  // Remove the task from the table of subscribed tasks.
  auto it = task_dependencies_.find(task_id);
  if (it == task_dependencies_.end()) {
    return false;
  }

  TaskDependencies &task_entry = it->second;

  auto wait_it = task_entry.wait_dependencies.find(object_id);
  if (wait_it == task_entry.wait_dependencies.end()) {
    return false;
  }
  task_entry.wait_dependencies.erase(wait_it);

  if (task_entry.get_dependencies.find(object_id) == task_entry.get_dependencies.end()) {
    RemoveTaskDependency(task_id, object_id);
  }

  HandleRemoteDependencyCanceled(object_id);

  if (task_entry.get_dependencies.empty() && task_entry.wait_dependencies.empty()) {
    task_dependencies_.erase(it);
  }

  return true;
}

std::vector<TaskID> TaskDependencyManager::GetPendingTasks() const {
  std::vector<TaskID> keys;
  keys.reserve(pending_tasks_.size());
  for (const auto &id_task_pair : pending_tasks_) {
    keys.push_back(id_task_pair.first);
  }
  return keys;
}

void TaskDependencyManager::TaskPending(const Task &task) {
  TaskID task_id = task.GetTaskSpecification().TaskId();

  // Record that the task is pending execution.
  auto inserted =
      pending_tasks_.emplace(task_id, PendingTask(initial_lease_period_ms_, io_service_));
  if (inserted.second) {
    // This is the first time we've heard that this task is pending.  Find any
    // subscribed tasks that are dependent on objects created by the pending
    // task.
    auto remote_task_entry = required_tasks_.find(task_id);
    if (remote_task_entry != required_tasks_.end()) {
      for (const auto &object_entry : remote_task_entry->second.required_objects) {
        // This object created by the pending task will appear locally once the
        // task completes execution. Cancel any in-progress operations to make
        // the object local.
        HandleRemoteDependencyCanceled(object_entry.first);
      }
    }

    if (!task.GetTaskSpecification().IsActorTask()) {
      // Acquire the lease for the task's execution in the global lease table.
      AcquireTaskLease(task_id);
    }
  }
}

void TaskDependencyManager::AcquireTaskLease(const TaskID &task_id) {
  auto it = pending_tasks_.find(task_id);
  int64_t now_ms = current_time_ms();
  if (it == pending_tasks_.end()) {
    return;
  }

  // Check that we were able to renew the task lease before the previous one
  // expired.
  if (now_ms > it->second.expires_at) {
    RAY_LOG(WARNING) << "Task lease to renew has already expired by "
                     << (it->second.expires_at - now_ms) << "ms";
  }

  auto task_lease_data = std::make_shared<TaskLeaseDataT>();
  task_lease_data->node_manager_id = client_id_.binary();
  task_lease_data->acquired_at = current_sys_time_ms();
  task_lease_data->timeout = it->second.lease_period;
  RAY_CHECK_OK(task_lease_table_.Add(DriverID::nil(), task_id, task_lease_data, nullptr));

  auto period = boost::posix_time::milliseconds(it->second.lease_period / 2);
  it->second.lease_timer->expires_from_now(period);
  it->second.lease_timer->async_wait(
      [this, task_id](const boost::system::error_code &error) {
        if (!error) {
          AcquireTaskLease(task_id);
        } else {
          // Check that the error was due to the timer being canceled.
          RAY_CHECK(error == boost::asio::error::operation_aborted);
        }
      });

  it->second.expires_at = now_ms + it->second.lease_period;
  it->second.lease_period = std::min(it->second.lease_period * 2,
                                     RayConfig::instance().max_task_lease_timeout_ms());
}

void TaskDependencyManager::TaskCanceled(const TaskID &task_id) {
  // Record that the task is no longer pending execution.
  auto it = pending_tasks_.find(task_id);
  if (it == pending_tasks_.end()) {
    return;
  }
  pending_tasks_.erase(it);

  // Find any subscribed tasks that are dependent on objects created by the
  // canceled task.
  auto remote_task_entry = required_tasks_.find(task_id);
  if (remote_task_entry != required_tasks_.end()) {
    for (const auto &object_entry : remote_task_entry->second.required_objects) {
      // This object created by the task will no longer appear locally since
      // the task is canceled.  Try to make the object local if necessary.
      HandleRemoteDependencyRequired(object_entry.first);
    }
  }
}

void TaskDependencyManager::RemoveTasksAndRelatedObjects(
    const std::unordered_set<TaskID> &task_ids) {
  // Collect a list of all the unique objects that these tasks were subscribed
  // to.
  std::unordered_set<ObjectID> required_objects;
  for (auto it = task_ids.begin(); it != task_ids.end(); it++) {
    auto task_it = task_dependencies_.find(*it);
    if (task_it != task_dependencies_.end()) {
      // Add the objects that this task was subscribed to.
      required_objects.insert(task_it->second.get_dependencies.begin(),
                              task_it->second.get_dependencies.end());
      required_objects.insert(task_it->second.wait_dependencies.begin(),
                              task_it->second.wait_dependencies.end());
    }
    // The task no longer depends on anything.
    task_dependencies_.erase(*it);
    // The task is no longer pending execution.
    pending_tasks_.erase(*it);
    RAY_LOG(DEBUG) << "Driver exited, task no longer required" << *it;
  }

  // Cancel all of the objects that were required by the removed tasks.
  for (const auto &object_id : required_objects) {
    TaskID creating_task_id = ComputeTaskId(object_id);
    required_tasks_.erase(creating_task_id);
    HandleRemoteDependencyCanceled(object_id);
    RAY_LOG(DEBUG) << "Driver exited, object no longer required" << object_id;
  }

  // Make sure that the tasks in task_ids no longer have tasks dependent on
  // them.
  for (const auto &task_id : task_ids) {
    RAY_CHECK(required_tasks_.find(task_id) == required_tasks_.end())
        << "RemoveTasksAndRelatedObjects was called on" << task_id
        << ", but another task depends on it that was not included in the argument";
  }
}

std::string TaskDependencyManager::DebugString() const {
  std::stringstream result;
  result << "TaskDependencyManager:";
  result << "\n- task dep map size: " << task_dependencies_.size();
  result << "\n- task req map size: " << required_tasks_.size();
  result << "\n- req objects map size: " << required_objects_.size();
  result << "\n- local objects map size: " << local_objects_.size();
  result << "\n- pending tasks map size: " << pending_tasks_.size();
  return result.str();
}

}  // namespace raylet

}  // namespace ray
