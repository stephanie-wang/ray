#include "task_dependency_manager.h"

namespace ray {

namespace raylet {

TaskDependencyManager::TaskDependencyManager(
    std::function<void(const ObjectID&, bool)> object_remote_handler,
    std::function<void(const ObjectID&)> cancel_object_remote_handler,
    std::function<void(const TaskID &)> task_ready_handler,
    std::function<void(const TaskID &)> task_waiting_handler)
    : object_remote_callback_(object_remote_handler),
      cancel_object_remote_callback_(cancel_object_remote_handler),
      task_ready_callback_(task_ready_handler),
      task_waiting_callback_(task_waiting_handler) {}

TaskDependencyManager::ObjectAvailability TaskDependencyManager::CheckObjectLocal(
    const ObjectID &object_id) const {
  auto entry = local_objects_.find(object_id);
  if (entry == local_objects_.end()) {
    return ObjectAvailability::kRemote;
  }
  return entry->second.status;
}

void TaskDependencyManager::MarkObjectAvailability(const ObjectID &object_id, ObjectAvailability availability) {
  local_objects_[object_id].status = availability;
  if (availability > ObjectAvailability::kRemote) {
    cancel_object_remote_callback_(object_id);
  }
}

std::unordered_set<TaskID, UniqueIDHasher> TaskDependencyManager::HandleObjectLocal(const ray::ObjectID &object_id) {
  RAY_LOG(DEBUG) << "object ready " << object_id.hex();

  // Add the object to the table of locally available objects.
  auto &object_entry = local_objects_[object_id];
  RAY_CHECK(object_entry.status != ObjectAvailability::kLocal);
  object_entry.status = ObjectAvailability::kLocal;

  // Find any tasks that are dependent on the newly available object.
  std::unordered_set<TaskID, UniqueIDHasher> ready_task_ids;
  for (auto &dependent_task_id : object_entry.dependent_tasks) {
    auto &task_entry = task_dependencies_[dependent_task_id];
    task_entry.num_missing_arguments--;
    // If the dependent task now has all of its arguments ready, it's ready
    // to run.
    if (task_entry.num_missing_arguments == 0) {
      ready_task_ids.insert(dependent_task_id);
    }
  }

  return ready_task_ids;
}

void TaskDependencyManager::HandleObjectMissing(const ray::ObjectID &object_id) {
  RAY_LOG(DEBUG) << "object ready " << object_id.hex();
  // Add the object to the table of locally available objects.
  auto &object_entry = local_objects_[object_id];
  RAY_CHECK(object_entry.status == ObjectAvailability::kLocal);
  object_entry.status = ObjectAvailability::kRemote;

  // Find any tasks that are dependent on the missing object.
  std::vector<TaskID> waiting_task_ids;
  for (auto &dependent_task_id : object_entry.dependent_tasks) {
    auto &task_entry = task_dependencies_[dependent_task_id];
    // If the dependent task had all of its arguments ready, it was ready to
    // run but must be switched to waiting since one of its arguments is now
    // missing.
    if (task_entry.num_missing_arguments == 0) {
      waiting_task_ids.push_back(dependent_task_id);
    }
    task_entry.num_missing_arguments++;
  }
  // Process callbacks for all of the tasks dependent on the object that are
  // now ready to run.
  // TODO(swang): We should not call this for tasks that have already started
  // executing.
  for (auto &waiting_task_id : waiting_task_ids) {
    task_waiting_callback_(waiting_task_id);
  }
}

bool TaskDependencyManager::SubscribeTask(const Task &task) {
  TaskID task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(INFO) << "Task dependencies subscribed " << task_id;
  TaskEntry task_entry;

  // Add the task's arguments to the table of subscribed tasks.
  task_entry.arguments = task.GetDependencies();
  int64_t execution_dependency_index = task_entry.arguments.size() - task.GetTaskExecutionSpecReadonly().ExecutionDependencies().size();
  task_entry.num_missing_arguments = task_entry.arguments.size();
  // Record the task as being dependent on each of its arguments.
  int64_t i = 0;
  for (const auto &argument_id : task_entry.arguments) {
    auto &argument_entry = local_objects_[argument_id];
    if (argument_entry.status == ObjectAvailability::kRemote) {
      bool request_transfer = i < execution_dependency_index;
      // Hack to prevent us from pulling dependencies for an execution
      // dependency.
      object_remote_callback_(argument_id, request_transfer);
    } else if (argument_entry.status == ObjectAvailability::kLocal) {
      task_entry.num_missing_arguments--;
    }
    argument_entry.dependent_tasks.push_back(task_id);
    i++;
  }

  for (int i = 0; i < task.GetTaskSpecification().NumReturns(); i++) {
    auto return_id = task.GetTaskSpecification().ReturnId(i);
    task_entry.returns.push_back(return_id);
  }
  // Record the task's return values as pending creation.
  for (const auto &return_id : task_entry.returns) {
    auto &return_entry = local_objects_[return_id];
    // Some of a task's return values may already be local if this is a
    // re-executed task and it created multiple objects, only some of which
    // needed to be reconstructed. We only want to mark the object as waiting
    // for creation if it was previously not available at all.
    if (return_entry.status < ObjectAvailability::kWaiting) {
      // The object does not already exist locally. Mark the object as
      // pending creation.
      return_entry.status = ObjectAvailability::kWaiting;
    }

    cancel_object_remote_callback_(return_id);
  }

  auto emplaced = task_dependencies_.emplace(task_id, task_entry);
  RAY_CHECK(emplaced.second);

  return (task_entry.num_missing_arguments == 0);
}

void TaskDependencyManager::UnsubscribeForwardedTask(const TaskID &task_id) {
  UnsubscribeTask(task_id, ObjectAvailability::kRemote);
}

void TaskDependencyManager::UnsubscribeExecutingTask(const TaskID &task_id) {
  UnsubscribeTask(task_id, ObjectAvailability::kConstructing);
}

void TaskDependencyManager::UnsubscribeTask(const TaskID &task_id,
                                            ObjectAvailability outputs_status) {
  // Remove the task from the table of subscribed tasks.
  auto it = task_dependencies_.find(task_id);
  RAY_CHECK(it != task_dependencies_.end());
  const TaskEntry task_entry = std::move(it->second);
  task_dependencies_.erase(it);

  // Remove the task from the table of objects to dependent tasks.
  for (const auto &argument_id : task_entry.arguments) {
    // Remove the task from the list of tasks that are dependent on this
    // object.
    auto argument_entry = local_objects_.find(argument_id);
    std::vector<TaskID> &dependent_tasks = argument_entry->second.dependent_tasks;
    for (auto it = dependent_tasks.begin(); it != dependent_tasks.end(); it++) {
      if (*it == task_id) {
        it = dependent_tasks.erase(it);
        break;
      }
    }
    if (dependent_tasks.empty()) {
      // There are no more tasks dependent on this object.
      if (argument_entry->second.status == ObjectAvailability::kRemote) {
        local_objects_.erase(argument_entry);
        cancel_object_remote_callback_(argument_id);
      }
    }
  }

  // Record the task's return values as remote.
  for (const auto &return_id : task_entry.returns) {
    auto return_entry = local_objects_.find(return_id);
    RAY_CHECK(return_entry != local_objects_.end());
    // Some of a task's return values may already be local if this is a
    // re-executed task and it created multiple objects, only some of which
    // needed to be reconstructed. We only want to update the object's status
    // if it was previously not available.
    if (return_entry->second.status != ObjectAvailability::kLocal) {
      RAY_CHECK(return_entry->second.status == ObjectAvailability::kWaiting);
      return_entry->second.status = outputs_status;
    }
    if (return_entry->second.status == ObjectAvailability::kRemote) {
      if (return_entry->second.dependent_tasks.empty()) {
        local_objects_.erase(return_entry);
      } else {
        object_remote_callback_(return_id, true);
      }
    }
  }
}

}  // namespace raylet

}  // namespace ray
