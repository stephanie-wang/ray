#ifndef RAY_CORE_WORKER_TASK_MANAGER_H
#define RAY_CORE_WORKER_TASK_MANAGER_H

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/protobuf/core_worker.pb.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

class TaskFinisherInterface {
 public:
  virtual void CompletePendingTask(const TaskID &task_id,
                                   const rpc::PushTaskReply &reply) = 0;

  virtual void FailPendingTask(const TaskID &task_id, rpc::ErrorType error_type) = 0;

  virtual ~TaskFinisherInterface() {}
};

class TaskManager : public TaskFinisherInterface {
 public:
  TaskManager(std::shared_ptr<CoreWorkerMemoryStore> in_memory_store,
              std::shared_ptr<gcs::RedisGcsClient> gcs_client = nullptr)
      : in_memory_store_(in_memory_store),
        gcs_client_(gcs_client) {
    if (gcs_client_) {
      RAY_LOG(INFO) << "** Using centralized owner!";
    }
  }

  /// Add a task that is pending execution.
  ///
  /// \param[in] spec The spec of the pending task.
  /// \return Void.
  void AddPendingTask(const TaskSpecification &spec);

  /// Return whether the task is pending.
  ///
  /// \param[in] task_id ID of the task to query.
  /// \return Whether the task is pending.
  bool IsTaskPending(const TaskID &task_id) const {
    return pending_tasks_.count(task_id) > 0;
  }

  /// Write return objects for a pending task to the memory store.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] reply Proto response to a direct actor or task call.
  /// \return Void.
  void CompletePendingTask(const TaskID &task_id,
                           const rpc::PushTaskReply &reply) override;

  /// Treat a pending task as failed.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] error_type The type of the specific error.
  /// \return Void.
  void FailPendingTask(const TaskID &task_id, rpc::ErrorType error_type) override;

 private:
  /// Used to store task results.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Protects below fields.
  absl::Mutex mu_;

  /// Map from task ID to the task's number of return values. This map contains
  /// one entry per pending task that we submitted.
  absl::flat_hash_map<TaskID, int64_t> pending_tasks_ GUARDED_BY(mu_);

  std::shared_ptr<gcs::RedisGcsClient> gcs_client_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_MANAGER_H
