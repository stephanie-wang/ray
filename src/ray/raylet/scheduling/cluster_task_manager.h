#pragma once

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/raylet/dependency_manager.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/cluster_task_manager_interface.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/rpc/node_manager/node_manager_server.h"

namespace ray {
namespace raylet {

/// Work represents all the information needed to make a scheduling decision.
/// This includes the task, the information we need to communicate to
/// dispatch/spillback and the callback to trigger it.
typedef std::tuple<Task, rpc::RequestWorkerLeaseReply *, std::function<void(void)>> Work;

typedef std::function<boost::optional<rpc::GcsNodeInfo>(const NodeID &node_id)>
    NodeInfoGetter;

/// Manages the queuing and dispatching of tasks. The logic is as follows:
/// 1. Queue tasks for scheduling.
/// 2. Pick a node on the cluster which has the available resources to run a
///    task.
///     * Step 2 should occur anytime any time the state of the cluster is
///       changed, or a new task is queued.
/// 3. If a task has unresolved dependencies, set it aside to wait for
///    dependencies to be resolved.
/// 4. When a task is ready to be dispatched, ensure that the local node is
///    still capable of running the task, then dispatch it.
///     * Step 4 should be run any time there is a new task to dispatch *or*
///       there is a new worker which can dispatch the tasks.
/// 5. When a worker finishes executing its task(s), the requester will return
///    it and we should release the resources in our view of the node's state.
class ClusterTaskManager : public ClusterTaskManagerInterface {
 public:
  /// fullfills_dependencies_func Should return if all dependencies are
  /// fulfilled and unsubscribe from dependencies only if they're fulfilled. If
  /// a task has dependencies which are not fulfilled, wait for the
  /// dependencies to be fulfilled, then run on the local node.
  ///
  /// \param self_node_id: ID of local node.
  /// \param cluster_resource_scheduler: The resource scheduler which contains
  /// the state of the cluster.
  /// \param task_dependency_manager_ Used to fetch task's dependencies.
  /// \param is_owner_alive: A callback which returns if the owner process is alive
  /// (according to our ownership model).
  /// \param gcs_client: A gcs client.
  ClusterTaskManager(
      const NodeID &self_node_id,
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
      TaskDependencyManagerInterface &task_dependency_manager,
      std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive,
      NodeInfoGetter get_node_info,
      std::function<void(const Task &)> announce_infeasible_task,
      WorkerPoolInterface &worker_pool,
      std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers,
      std::function<bool(const std::vector<ObjectID> &object_ids,
                         std::vector<std::unique_ptr<RayObject>> *results)>
          get_task_arguments,
      size_t max_pinned_task_arguments_bytes);

  /// (Step 1) Queue tasks and schedule.
  /// Queue task and schedule. This hanppens when processing the worker lease request.
  ///
  /// \param task: The incoming task to be queued and scheduled.
  /// \param reply: The reply of the lease request.
  /// \param send_reply_callback: The function used during dispatching.
  void QueueAndScheduleTask(const Task &task, rpc::RequestWorkerLeaseReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// Move tasks from waiting to ready for dispatch. Called when a task's
  /// dependencies are resolved.
  ///
  /// \param readyIds: The tasks which are now ready to be dispatched.
  void TasksUnblocked(const std::vector<TaskID> &ready_ids) override;

  /// Return the finished task and relase the worker resources.
  /// This method will be removed and can be replaced by `ReleaseWorkerResources` directly
  /// once we remove the legacy scheduler.
  ///
  /// \param worker: The worker which was running the task.
  /// \param task: Output parameter.
  void TaskFinished(std::shared_ptr<WorkerInterface> worker, Task *task) override;

  /// Return worker resources.
  /// This method will be removed and can be replaced by `ReleaseWorkerResources` directly
  /// once we remove the legacy scheduler.
  ///
  /// \param worker: The worker which was running the task.
  void ReturnWorkerResources(std::shared_ptr<WorkerInterface> worker) override;

  /// Attempt to cancel an already queued task.
  ///
  /// \param task_id: The id of the task to remove.
  /// \param runtime_env_setup_failed: If this is being cancelled because the env setup
  /// failed.
  ///
  /// \return True if task was successfully removed. This function will return
  /// false if the task is already running.
  bool CancelTask(const TaskID &task_id, bool runtime_env_setup_failed = false) override;

  /// Populate the list of pending or infeasible actor tasks for node stats.
  ///
  /// \param Output parameter.
  void FillPendingActorInfo(rpc::GetNodeStatsReply *reply) const override;

  /// Populate the relevant parts of the heartbeat table. This is intended for
  /// sending resource usage of raylet to gcs. In particular, this should fill in
  /// resource_load and resource_load_by_shape.
  ///
  /// \param Output parameter. `resource_load` and `resource_load_by_shape` are the only
  /// fields used.
  void FillResourceUsage(rpc::ResourcesData &data,
                         const std::shared_ptr<SchedulingResources>
                             &last_reported_resources = nullptr) override;

  /// Return if any tasks are pending resource acquisition.
  ///
  /// \param[in] exemplar An example task that is deadlocking.
  /// \param[in] num_pending_actor_creation Number of pending actor creation tasks.
  /// \param[in] num_pending_tasks Number of pending tasks.
  /// \param[in] any_pending True if there's any pending exemplar.
  /// \return True if any progress is any tasks are pending.
  bool AnyPendingTasks(Task *exemplar, bool *any_pending, int *num_pending_actor_creation,
                       int *num_pending_tasks) const override;

  /// (Step 5) Call once a task finishes (i.e. a worker is returned).
  ///
  /// \param worker: The worker which was running the task.
  void ReleaseWorkerResources(std::shared_ptr<WorkerInterface> worker) override;

  /// When a task is blocked in ray.get or ray.wait, the worker who is executing the task
  /// should give up the CPU resources allocated for the running task for the time being
  /// and the worker itself should also be marked as blocked.
  ///
  /// \param worker The worker who will give up the CPU resources.
  /// \return true if the cpu resources of the specified worker are released successfully,
  /// else false.
  bool ReleaseCpuResourcesFromUnblockedWorker(
      std::shared_ptr<WorkerInterface> worker) override;

  /// When a task is no longer blocked in a ray.get or ray.wait, the CPU resources that
  /// the worker gave up should be returned to it.
  ///
  /// \param worker The blocked worker.
  /// \return true if the cpu resources are returned back to the specified worker, else
  /// false.
  bool ReturnCpuResourcesToBlockedWorker(
      std::shared_ptr<WorkerInterface> worker) override;

  // Schedule and dispatch tasks.
  void ScheduleAndDispatchTasks() override;

  void RecordMetrics() override;

  /// The helper to dump the debug state of the cluster task manater.
  std::string DebugStr() const override;

  /// Calculate normal task resources.
  ResourceSet CalcNormalTaskResources() const override;

 private:
  /// (Step 2) For each task in tasks_to_schedule_, pick a node in the system
  /// (local or remote) that has enough resources available to run the task, if
  /// any such node exist. Skip tasks which are not schedulable.
  ///
  /// \return True if any tasks are ready for dispatch.
  bool SchedulePendingTasks();

  /// (Step 3) Attempts to dispatch all tasks which are ready to run. A task
  /// will be dispatched if it is on `tasks_to_dispatch_` and there are still
  /// available resources on the node.
  ///
  /// If there are not enough resources locally, up to one task per resource
  /// shape (the task at the head of the queue) will get spilled back to a
  /// different node.
  void DispatchScheduledTasksToWorkers(
      WorkerPoolInterface &worker_pool,
      std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers);

  /// Helper method when the current node does not have the available resources to run a
  /// task.
  ///
  /// \returns true if the task was spilled. The task may not be spilled if the
  /// spillback policy specifies the local node (which may happen if no other nodes have
  /// the available resources).
  bool TrySpillback(const Work &spec, bool &is_infeasible);

  /// Helper method to try dispatching a single task from the queue to an
  /// available worker. Returns whether the task should be removed from the
  /// queue and whether the worker was successfully leased to execute the work.
  bool AttemptDispatchWork(const Work &work, std::shared_ptr<WorkerInterface> &worker,
                           bool *worker_leased);

  /// Reiterate all local infeasible tasks and register them to task_to_schedule_ if it
  /// becomes feasible to schedule.
  void TryLocalInfeasibleTaskScheduling();

  // Try to spill waiting tasks to a remote node, starting from the end of the
  // queue.
  void SpillWaitingTasks();

  const NodeID &self_node_id_;
  /// Responsible for resource tracking/view of the cluster.
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  /// Class to make task dependencies to be local.
  TaskDependencyManagerInterface &task_dependency_manager_;
  /// Function to check if the owner is alive on a given node.
  std::function<bool(const WorkerID &, const NodeID &)> is_owner_alive_;
  /// Function to get the node information of a given node id.
  NodeInfoGetter get_node_info_;
  /// Function to announce infeasible task to GCS.
  std::function<void(const Task &)> announce_infeasible_task_;

  const int max_resource_shapes_per_load_report_;
  const bool report_worker_backlog_;

  /// TODO(swang): Add index from TaskID -> Work to avoid having to iterate
  /// through queues to cancel tasks, etc.
  /// Queue of lease requests that are waiting for resources to become available.
  /// Tasks move from scheduled -> dispatch | waiting.
  std::unordered_map<SchedulingClass, absl::btree_map<TaskKey, Work>> tasks_to_schedule_;

  /// Queue of lease requests that should be scheduled onto workers.
  /// Tasks move from scheduled | waiting -> dispatch.
  /// Tasks can also move from dispatch -> waiting if one of their arguments is
  /// evicted.
  /// All tasks in this map that have dependencies should be registered with
  /// the dependency manager, in case a dependency gets evicted while the task
  /// is still queued.
  std::unordered_map<SchedulingClass, absl::btree_map<TaskKey, Work>> tasks_to_dispatch_;

  /// Tasks waiting for arguments to be transferred locally.
  /// Tasks move from waiting -> dispatch.
  /// Tasks can also move from dispatch -> waiting if one of their arguments is
  /// evicted.
  /// All tasks in this map that have dependencies should be registered with
  /// the dependency manager, so that they can be moved to dispatch once their
  /// dependencies are local.
  ///
  /// We keep these in a queue so that tasks can be spilled back from the end
  /// of the queue. This is to try to prioritize spilling tasks whose
  /// dependencies may not be fetched locally yet.
  ///
  /// Note that because tasks can also move from dispatch -> waiting, the order
  /// in this queue may not match the order in which we initially received the
  /// tasks. This also means that the PullManager may request dependencies for
  /// these tasks in a different order than the waiting task queue.
  std::list<Work> waiting_task_queue_;

  /// An index for the above queue.
  absl::flat_hash_map<TaskID, std::list<Work>::iterator> waiting_tasks_index_;

  /// Queue of lease requests that are infeasible.
  /// Tasks go between scheduling <-> infeasible.
  std::unordered_map<SchedulingClass, absl::btree_map<TaskKey, Work>> infeasible_tasks_;

  /// Track the cumulative backlog of all workers requesting a lease to this raylet.
  std::unordered_map<SchedulingClass, int> backlog_tracker_;

  /// TODO(Shanly): Remove `worker_pool_` and `leased_workers_` and make them as
  /// parameters of methods if necessary once we remove the legacy scheduler.
  WorkerPoolInterface &worker_pool_;
  std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers_;

  /// Callback to get references to task arguments. These will be pinned while
  /// the task is running.
  std::function<bool(const std::vector<ObjectID> &object_ids,
                     std::vector<std::unique_ptr<RayObject>> *results)>
      get_task_arguments_;

  /// Arguments needed by currently granted lease requests. These should be
  /// pinned before the lease is granted to ensure that the arguments are not
  /// evicted before the task(s) start running.
  std::unordered_map<TaskID, std::vector<ObjectID>> executing_task_args_;

  /// All arguments of running tasks, which are also pinned in the object
  /// store. The value is a pair: (the pointer to the object store that should
  /// be deleted once the object is no longer needed, number of tasks that
  /// depend on the object).
  std::unordered_map<ObjectID, std::pair<std::unique_ptr<RayObject>, size_t>>
      pinned_task_arguments_;

  /// The total number of arguments pinned for running tasks.
  /// Used for debug purposes.
  size_t pinned_task_arguments_bytes_ = 0;

  /// The maximum amount of bytes that can be used by executing task arguments.
  size_t max_pinned_task_arguments_bytes_;

  /// Metrics collected since the last report.
  uint64_t metric_tasks_queued_;
  uint64_t metric_tasks_dispatched_;
  uint64_t metric_tasks_spilled_;

  /// Determine whether a task should be immediately dispatched,
  /// or placed on a wait queue.
  ///
  /// \return True if the work can be immediately dispatched.
  bool WaitForTaskArgsRequests(Work work);

  void Dispatch(
      std::shared_ptr<WorkerInterface> worker,
      std::unordered_map<WorkerID, std::shared_ptr<WorkerInterface>> &leased_workers_,
      std::shared_ptr<TaskResourceInstances> &allocated_instances, const Task &task,
      rpc::RequestWorkerLeaseReply *reply, std::function<void(void)> send_reply_callback);

  void Spillback(const NodeID &spillback_to, const Work &work);

  void AddToBacklogTracker(const Task &task);
  void RemoveFromBacklogTracker(const Task &task);

  // Helper function to pin a task's args immediately before dispatch. This
  // returns false if there are missing args (due to eviction) or if there is
  // not enough memory available to dispatch the task, due to other executing
  // tasks' arguments.
  bool PinTaskArgsIfMemoryAvailable(const TaskSpecification &spec, bool *args_missing);

  // Helper functions to pin and release an executing task's args.
  void PinTaskArgs(const TaskSpecification &spec,
                   std::vector<std::unique_ptr<RayObject>> args);
  void ReleaseTaskArgs(const TaskID &task_id);

  friend class ClusterTaskManagerTest;
  FRIEND_TEST(ClusterTaskManagerTest, FeasibleToNonFeasible);
};
}  // namespace raylet
}  // namespace ray
