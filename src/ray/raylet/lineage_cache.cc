#include "lineage_cache.h"

#include <chrono>

namespace ray {

namespace raylet {

LineageEntry::LineageEntry(const Task &task, GcsStatus status)
    : status_(status), task_(task) {}

GcsStatus LineageEntry::GetStatus() const { return status_; }

bool LineageEntry::SetStatus(GcsStatus new_status) {
  if (status_ < new_status) {
    status_ = new_status;
    return true;
  } else {
    return false;
  }
}

void LineageEntry::ResetStatus(GcsStatus new_status) {
  RAY_CHECK(new_status < status_);
  status_ = new_status;
}

const TaskID LineageEntry::GetEntryId() const {
  return task_.GetTaskSpecification().TaskId();
}

const std::unordered_set<UniqueID, UniqueIDHasher> LineageEntry::GetParentTaskIds()
    const {
  std::unordered_set<UniqueID, UniqueIDHasher> parent_ids;
  // A task's parents are the tasks that created its arguments.
  auto dependencies = task_.GetDependencies();
  for (auto &dependency : dependencies) {
    auto parent_id = ComputeTaskId(dependency);
    // TODO(swang): wtf
    if (!(parent_id == task_.GetTaskSpecification().TaskId())) {
      parent_ids.insert(parent_id);
    }
  }
  return parent_ids;
}

const Task &LineageEntry::TaskData() const { return task_; }

Task &LineageEntry::TaskDataMutable() { return task_; }

Lineage::Lineage() {}

Lineage::Lineage(const protocol::ForwardTaskRequest &task_request) {
  // Deserialize and set entries for the uncommitted tasks.
  auto tasks = task_request.uncommitted_tasks();
  for (auto it = tasks->begin(); it != tasks->end(); it++) {
    auto task = Task(**it);
    LineageEntry entry(task, GcsStatus_UNCOMMITTED_REMOTE);
    RAY_CHECK(SetEntry(std::move(entry)));
  }
}

boost::optional<const LineageEntry &> Lineage::GetEntry(const UniqueID &task_id) const {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    return entry->second;
  } else {
    return boost::optional<const LineageEntry &>();
  }
}

boost::optional<LineageEntry &> Lineage::GetEntryMutable(const UniqueID &task_id) {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    return entry->second;
  } else {
    return boost::optional<LineageEntry &>();
  }
}

bool Lineage::SetEntry(LineageEntry &&new_entry) {
  // Get the status of the current entry at the key.
  auto task_id = new_entry.GetEntryId();
  GcsStatus current_status = GcsStatus_NONE;
  auto current_entry = PopEntry(task_id);
  if (current_entry) {
    current_status = current_entry->GetStatus();
  }

  if (current_status < new_entry.GetStatus()) {
    // If the new status is greater, then overwrite the current entry.
    entries_.emplace(std::make_pair(task_id, std::move(new_entry)));
    return true;
  } else {
    // If the new status is not greater, then the new entry is invalid. Replace
    // the current entry at the key.
    entries_.emplace(std::make_pair(task_id, std::move(*current_entry)));
    return false;
  }
}

boost::optional<LineageEntry> Lineage::PopEntry(const UniqueID &task_id) {
  auto entry = entries_.find(task_id);
  if (entry != entries_.end()) {
    LineageEntry entry = std::move(entries_.at(task_id));
    entries_.erase(task_id);
    return entry;
  } else {
    return boost::optional<LineageEntry>();
  }
}

const std::unordered_map<const UniqueID, LineageEntry, UniqueIDHasher>
    &Lineage::GetEntries() const {
  return entries_;
}

flatbuffers::Offset<protocol::ForwardTaskRequest> Lineage::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb, const TaskID &task_id) const {
  RAY_CHECK(GetEntry(task_id));
  // Serialize the task and object entries.
  std::vector<flatbuffers::Offset<protocol::Task>> uncommitted_tasks;
  for (const auto &entry : entries_) {
    uncommitted_tasks.push_back(entry.second.TaskData().ToFlatbuffer(fbb));
  }

  auto request = protocol::CreateForwardTaskRequest(fbb, to_flatbuf(fbb, task_id),
                                                    fbb.CreateVector(uncommitted_tasks));
  return request;
}

LineageCache::LineageCache(const ClientID &client_id,
                           gcs::TableInterface<TaskID, protocol::Task> &task_storage,
                           gcs::PubsubInterface<TaskID> &task_pubsub)
    : client_id_(client_id), task_storage_(task_storage), task_pubsub_(task_pubsub) {}

/// A helper function to merge one lineage into another, in DFS order.
///
/// \param task_id The current entry to merge from lineage_from into
/// lineage_to.
/// \param lineage_from The lineage to merge entries from. This lineage is
/// traversed by following each entry's parent pointers in DFS order,
/// until an entry is not found or the stopping condition is reached.
/// \param lineage_to The lineage to merge entries into.
/// \param stopping_condition A stopping condition for the DFS over
/// lineage_from. This should return true if the merge should stop.
void MergeLineageHelper(const UniqueID &task_id, const Lineage &lineage_from,
                        Lineage &lineage_to,
                        std::function<bool(GcsStatus)> stopping_condition) {
  // If the entry is not found in the lineage to merge, then we stop since
  // there is nothing to copy into the merged lineage.
  auto entry = lineage_from.GetEntry(task_id);
  if (!entry) {
    return;
  }
  // Check whether we should stop at this entry in the DFS.
  auto status = entry->GetStatus();
  if (stopping_condition(status)) {
    return;
  }

  // Insert a copy of the entry into lineage_to.
  LineageEntry entry_copy = *entry;
  auto parent_ids = entry_copy.GetParentTaskIds();
  // If the insert is successful, then continue the DFS. The insert will fail
  // if the new entry has an equal or lower GCS status than the current entry
  // in lineage_to. This also prevents us from traversing the same node twice.
  if (lineage_to.SetEntry(std::move(entry_copy))) {
    for (const auto &parent_id : parent_ids) {
      RAY_LOG(DEBUG) << "parent task: " << parent_id;
      MergeLineageHelper(parent_id, lineage_from, lineage_to, stopping_condition);
    }
  }
}

void LineageCache::AddWaitingTask(const Task &task, const Lineage &uncommitted_lineage) {
  auto task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(DEBUG) << "Adding waiting task " << task_id;
  // Merge the uncommitted lineage into the lineage cache.
  MergeLineageHelper(task_id, uncommitted_lineage, lineage_, [](GcsStatus status) {
    if (status != GcsStatus_NONE) {
      // We received the uncommitted lineage from a remote node, so make sure
      // that all entries in the lineage to merge have status
      // UNCOMMITTED_REMOTE.
      RAY_CHECK(status == GcsStatus_UNCOMMITTED_REMOTE) << "status was: " << status;
    }
    // The only stopping condition is that an entry is not found.
    return false;
  });

  // Add the submitted task to the lineage cache as UNCOMMITTED_WAITING. It
  // should be marked as UNCOMMITTED_READY once the task starts execution.
  LineageEntry task_entry(task, GcsStatus_UNCOMMITTED_WAITING);
  RAY_CHECK(lineage_.SetEntry(std::move(task_entry)))
      << "Task " << task_id << " status was " << lineage_.GetEntry(task_id)->GetStatus();
}

void LineageCache::AddReadyTask(const Task &task) {
  auto new_entry = LineageEntry(task, GcsStatus_UNCOMMITTED_READY);
  RAY_CHECK(lineage_.SetEntry(std::move(new_entry)));
  // Add the task to the cache of tasks that may be flushed.
  uncommitted_ready_tasks_.insert(task.GetTaskSpecification().TaskId());

  // Try to flush the task to the GCS.
  // TODO(swang): Allow a pluggable policy for when to flush.
  RAY_CHECK_OK(Flush());
}

void LineageCache::RemoveWaitingTask(const TaskID &task_id) {
  auto entry = lineage_.PopEntry(task_id);
  // It's only okay to remove a task that is waiting for execution.
  // TODO(swang): Is this necessarily true when there is reconstruction?
  RAY_CHECK(entry->GetStatus() == GcsStatus_UNCOMMITTED_WAITING) << "status was " << entry->GetStatus();
  // Reset the status to REMOTE. We keep the task instead of removing it
  // completely in case another task is submitted locally that depends on this
  // one.
  entry->ResetStatus(GcsStatus_UNCOMMITTED_REMOTE);
  RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
}

Lineage LineageCache::GetUncommittedLineage(const TaskID &task_id) {
  Lineage uncommitted_lineage;
  // Add all uncommitted ancestors from the lineage cache to the uncommitted
  // lineage of the requested task.
  MergeLineageHelper(task_id, lineage_, uncommitted_lineage, [](GcsStatus status) {
    // The stopping condition for recursion is that the entry has been
    // committed to the GCS.
    return status == GcsStatus_COMMITTED;
  });

  size_t uncommitted_lineage_size = uncommitted_lineage.GetEntries().size();
  if (uncommitted_lineage_size % 100 && uncommitted_lineage_size > 100) {
    auto task = uncommitted_lineage.GetEntry(task_id);
    RAY_CHECK(task);
    for (const auto &parent_id : task->GetParentTaskIds()) {
      auto parent = lineage_.GetEntry(parent_id);
      if (parent && parent->GetStatus() == GcsStatus_UNCOMMITTED_REMOTE) {
        // Request notifications about the parent entry's commit in the GCS.
        // Once we receive a notification about the task's commit via
        // HandleEntryCommitted, then this task will be ready to write on the
        // next call to Flush().
        auto inserted = subscribed_tasks_.insert(parent_id);
        if (inserted.second) {
          RAY_LOG(INFO) << "subscribing to " << parent_id
                        << " uncommitted lineage for task " << task_id << " has size "
                        << uncommitted_lineage_size;
          RAY_CHECK_OK(
              task_pubsub_.RequestNotifications(JobID::nil(), parent_id, client_id_));
        }
      }
    }
  }

  return uncommitted_lineage;
}

Status LineageCache::Flush() {
  // Iterate through all tasks that are READY.
  std::vector<TaskID> ready_task_ids;
  for (const auto &task_id : uncommitted_ready_tasks_) {
    auto entry = lineage_.GetEntry(task_id);
    RAY_CHECK(entry);
    RAY_CHECK(entry->GetStatus() == GcsStatus_UNCOMMITTED_READY);

    // Check if all arguments have been committed to the GCS before writing
    // this task.
    bool all_arguments_committed = true;
    for (const auto &parent_id : entry->GetParentTaskIds()) {
      auto parent = lineage_.GetEntry(parent_id);
      // If a parent entry exists in the lineage cache but has not been
      // committed yet, then as far as we know, it's still in flight to the
      // GCS. Skip this task for now.
      if (parent && parent->GetStatus() != GcsStatus_COMMITTED) {
        // Children should not become ready to flush before their parents.
        RAY_CHECK(parent->GetStatus() != GcsStatus_UNCOMMITTED_WAITING);
        if (parent->GetStatus() == GcsStatus_UNCOMMITTED_REMOTE) {
          // Request notifications about the parent entry's commit in the GCS.
          // Once we receive a notification about the task's commit via
          // HandleEntryCommitted, then this task will be ready to write on the
          // next call to Flush().
          auto inserted = subscribed_tasks_.insert(parent_id);
          if (inserted.second) {
            std::chrono::milliseconds start =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch());
            RAY_LOG(INFO) << "subscribing to " << parent_id << " before writing "
                          << task_id << " at " << start.count();

            RAY_CHECK_OK(
                task_pubsub_.RequestNotifications(JobID::nil(), parent_id, client_id_));
          }
        }
        RAY_LOG(INFO) << "task " << task_id << " has parent " << parent_id;
        task_children_[parent_id].insert(task_id);
        all_arguments_committed = false;
        //break;
      }
    }
    if (all_arguments_committed) {
      // All arguments have been committed to the GCS. Add this task to the
      // list of tasks to write back to the GCS.
      ready_task_ids.push_back(task_id);
    }
  }

  // Write back all ready tasks whose arguments have been committed to the GCS.
  gcs::raylet::TaskTable::WriteCallback task_callback = [this](
      ray::gcs::AsyncGcsClient *client, const TaskID &id,
      const std::shared_ptr<protocol::TaskT> data) { HandleEntryCommitted(id); };
  for (const auto &ready_task_id : ready_task_ids) {
    auto task = lineage_.GetEntry(ready_task_id);
    // TODO(swang): Make this better...
    flatbuffers::FlatBufferBuilder fbb;
    auto message = task->TaskData().ToFlatbuffer(fbb);
    fbb.Finish(message);
    auto task_data = std::make_shared<protocol::TaskT>();
    auto root = flatbuffers::GetRoot<protocol::Task>(fbb.GetBufferPointer());
    root->UnPackTo(task_data.get());
    RAY_LOG(DEBUG) << "task committing: " << ready_task_id;
    RAY_CHECK_OK(task_storage_.Add(task->TaskData().GetTaskSpecification().DriverId(),
                                   ready_task_id, task_data, task_callback));

    // We successfully wrote the task, so mark it as committing.
    // TODO(swang): Use a batched interface and write with all object entries.
    auto entry = lineage_.PopEntry(ready_task_id);
    RAY_CHECK(entry->SetStatus(GcsStatus_COMMITTING));
    RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
    // Erase the task from the cache of uncommitted ready tasks.
    uncommitted_ready_tasks_.erase(ready_task_id);

    std::chrono::milliseconds start =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    RAY_LOG(INFO) << "committing task " << ready_task_id << " at " << start.count();
  }

  return ray::Status::OK();
}

bool LineageCache::FlushTask(const TaskID &task_id) {
  auto entry = lineage_.GetEntry(task_id);
  RAY_CHECK(entry);
  RAY_CHECK(entry->GetStatus() == GcsStatus_UNCOMMITTED_READY);

  // Check if all arguments have been committed to the GCS before writing
  // this task.
  bool all_arguments_committed = true;
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    auto parent = lineage_.GetEntry(parent_id);
    // If a parent entry exists in the lineage cache but has not been
    // committed yet, then as far as we know, it's still in flight to the
    // GCS. Skip this task for now.
    if (parent && parent->GetStatus() != GcsStatus_COMMITTED) {
      // Children should not become ready to flush before their parents.
      RAY_CHECK(parent->GetStatus() != GcsStatus_UNCOMMITTED_WAITING);
      if (parent->GetStatus() == GcsStatus_UNCOMMITTED_REMOTE) {
        // Request notifications about the parent entry's commit in the GCS.
        // Once we receive a notification about the task's commit via
        // HandleEntryCommitted, then this task will be ready to write on the
        // next call to Flush().
        auto inserted = subscribed_tasks_.insert(parent_id);
        if (inserted.second) {
          std::chrono::milliseconds start =
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch());
          RAY_LOG(INFO) << "subscribing to " << parent_id << " before writing "
                        << task_id << " at " << start.count();

          RAY_CHECK_OK(
              task_pubsub_.RequestNotifications(JobID::nil(), parent_id, client_id_));
        }
      }
      all_arguments_committed = false;
      break;
    }
  }

  if (all_arguments_committed) {
    gcs::raylet::TaskTable::WriteCallback task_callback = [this](
        ray::gcs::AsyncGcsClient *client, const TaskID &id,
        const std::shared_ptr<protocol::TaskT> data) { HandleEntryCommitted(id); };

    auto task = lineage_.GetEntry(task_id);
    // TODO(swang): Make this better...
    flatbuffers::FlatBufferBuilder fbb;
    auto message = task->TaskData().ToFlatbuffer(fbb);
    fbb.Finish(message);
    auto task_data = std::make_shared<protocol::TaskT>();
    auto root = flatbuffers::GetRoot<protocol::Task>(fbb.GetBufferPointer());
    root->UnPackTo(task_data.get());
    RAY_LOG(DEBUG) << "task committing: " << task_id;
    RAY_CHECK_OK(task_storage_.Add(task->TaskData().GetTaskSpecification().DriverId(),
                                   task_id, task_data, task_callback));

    // We successfully wrote the task, so mark it as committing.
    // TODO(swang): Use a batched interface and write with all object entries.
    auto entry = lineage_.PopEntry(task_id);
    RAY_CHECK(entry->SetStatus(GcsStatus_COMMITTING));
    RAY_CHECK(lineage_.SetEntry(std::move(*entry)));
    // Erase the task from the cache of uncommitted ready tasks.
    uncommitted_ready_tasks_.erase(task_id);

    std::chrono::milliseconds start =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    RAY_LOG(INFO) << "committing individual task " << task_id << " at " << start.count();
  }

  return all_arguments_committed;
}

int64_t PopAncestorTasks(const UniqueID &task_id, Lineage &lineage) {
  int64_t popped = 0;
  auto entry = lineage.PopEntry(task_id);
  if (!entry) {
    return popped;
  }
  popped++;
  RAY_LOG(DEBUG) << "task removed: " << task_id;
  auto status = entry->GetStatus();
  RAY_CHECK(status == GcsStatus_UNCOMMITTED_REMOTE || status == GcsStatus_COMMITTED);
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    popped = popped + PopAncestorTasks(parent_id, lineage);
  }
  return popped;
}

void LineageCache::HandleEntryCommitted(const UniqueID &task_id) {
  RAY_LOG(DEBUG) << "task committed: " << task_id;
  auto entry = lineage_.PopEntry(task_id);
  if (!entry) {
    // TODO(swang): Is this safe?
    return;
  }

  int64_t popped = 0;
  for (const auto &parent_id : entry->GetParentTaskIds()) {
    popped = popped + PopAncestorTasks(parent_id, lineage_);
  }

  std::chrono::milliseconds start = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  RAY_LOG(INFO) << "task committed " << task_id << ", removed " << popped << " at " << start.count();

  // Mark this task as COMMITTED. Any tasks that were dependent on it and are
  // ready to be written may now be flushed to the GCS.
  bool committed = entry->SetStatus(GcsStatus_COMMITTED);
  if (!committed) {
    // If we failed to mark the task as committed, check that it's because it
    // was committed before. This means that we already received a notification
    // about the commit.
    RAY_CHECK(entry->GetStatus() == GcsStatus_COMMITTED);
  }
  RAY_CHECK(lineage_.SetEntry(std::move(*entry)));

  // Stop listening for notifications about this task.
  auto it = subscribed_tasks_.find(task_id);
  if (it != subscribed_tasks_.end()) {
    RAY_CHECK_OK(task_pubsub_.CancelNotifications(JobID::nil(), task_id, client_id_));
    subscribed_tasks_.erase(it);
  }

  for (const auto &child_id : task_children_[task_id]) {
    RAY_LOG(INFO) << "task " << task_id << " has child " << child_id;
    FlushTask(child_id);
  }
  task_children_.erase(task_id);
}

}  // namespace raylet

}  // namespace ray
