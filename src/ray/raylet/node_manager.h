#ifndef RAY_RAYLET_NODE_MANAGER_H
#define RAY_RAYLET_NODE_MANAGER_H

#include <boost/asio/steady_timer.hpp>

// clang-format off
#include "ray/raylet/task.h"
#include "ray/object_manager/object_manager.h"
#include "ray/common/client_connection.h"
#include "ray/gcs/format/util.h"
#include "ray/raylet/actor_registration.h"
#include "ray/raylet/lineage_cache.h"
#include "ray/raylet/scheduling_policy.h"
#include "ray/raylet/scheduling_queue.h"
#include "ray/raylet/scheduling_resources.h"
#include "ray/raylet/reconstruction_policy.h"
#include "ray/raylet/task_dependency_manager.h"
#include "ray/raylet/worker_pool.h"
#include "ray/util/ordered_set.h"
// clang-format on

namespace ray {

namespace raylet {

struct NodeManagerConfig {
  /// The node's resource configuration.
  ResourceSet resource_config;
  /// The port to use for listening to incoming connections. If this is 0 then
  /// the node manager will choose its own port.
  int node_manager_port;
  /// The initial number of workers to create.
  int num_initial_workers;
  /// The number of workers per process.
  int num_workers_per_process;
  /// The maximum number of workers that can be started concurrently by a
  /// worker pool.
  int maximum_startup_concurrency;
  /// The commands used to start the worker process, grouped by language.
  std::unordered_map<Language, std::vector<std::string>> worker_commands;
  /// The time between heartbeats in milliseconds.
  uint64_t heartbeat_period_ms;
  /// The time between debug dumps in milliseconds, or -1 to disable.
  uint64_t debug_dump_period_ms;
  /// the maximum lineage size.
  uint64_t max_lineage_size;
  /// The store socket name.
  std::string store_socket_name;
  /// The path to the ray temp dir.
  std::string temp_dir;
  int gcs_delay_ms;
  bool use_gcs_only;
};

class NodeManager {
 public:
  /// Create a node manager.
  ///
  /// \param resource_config The initial set of node resources.
  /// \param object_manager A reference to the local object manager.
  NodeManager(boost::asio::io_service &io_service, const NodeManagerConfig &config,
              ObjectManager &object_manager,
              std::shared_ptr<gcs::AsyncGcsClient> gcs_client,
              std::shared_ptr<ObjectDirectoryInterface> object_directory_);

  /// Process a new client connection.
  ///
  /// \param client The client to process.
  /// \return Void.
  void ProcessNewClient(LocalClientConnection &client);

  /// Process a message from a client. This method is responsible for
  /// explicitly listening for more messages from the client if the client is
  /// still alive.
  ///
  /// \param client The client that sent the message.
  /// \param message_type The message type (e.g., a flatbuffer enum).
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessClientMessage(const std::shared_ptr<LocalClientConnection> &client,
                            int64_t message_type, const uint8_t *message_data);

  /// Handle a new node manager connection.
  ///
  /// \param node_manager_client The connection to the remote node manager.
  /// \return Void.
  void ProcessNewNodeManager(TcpClientConnection &node_manager_client);

  /// Handle a message from a remote node manager.
  ///
  /// \param node_manager_client The connection to the remote node manager.
  /// \param message_type The type of the message.
  /// \param message The message contents.
  /// \return Void.
  void ProcessNodeManagerMessage(TcpClientConnection &node_manager_client,
                                 int64_t message_type, const uint8_t *message);

  /// Subscribe to the relevant GCS tables and set up handlers.
  ///
  /// \return Status indicating whether this was done successfully or not.
  ray::Status RegisterGcs();

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

 private:
  /// Methods for handling clients.

  /// Handler for the addition of a new GCS client.
  ///
  /// \param data Data associated with the new client.
  /// \return Void.
  void ClientAdded(const ClientTableDataT &data);
  /// Handler for the removal of a GCS client.
  /// \param client_data Data associated with the removed client.
  /// \return Void.
  void ClientRemoved(const ClientTableDataT &client_data);

  void HandleFlushAllCompleted();

  /// Send heartbeats to the GCS.
  void Heartbeat();

  /// Write out debug state to a file.
  void DumpDebugState();

  /// Get profiling information from the object manager and push it to the GCS.
  ///
  /// \return Void.
  void GetObjectManagerProfileInfo();

  /// Handler for a heartbeat notification from the GCS.
  ///
  /// \param id The ID of the node manager that sent the heartbeat.
  /// \param data The heartbeat data including load information.
  /// \return Void.
  void HeartbeatAdded(const ClientID &id, const HeartbeatTableDataT &data);
  /// Handler for a heartbeat batch notification from the GCS
  ///
  /// \param heartbeat_batch The batch of heartbeat data.
  void HeartbeatBatchAdded(const HeartbeatBatchTableDataT &heartbeat_batch);

  /// Methods for task scheduling.

  /// Enqueue a placeable task to wait on object dependencies or be ready for
  /// dispatch.
  ///
  /// \param task The task in question.
  /// \return Void.
  void EnqueuePlaceableTask(const Task &task, bool push);
  void EnqueuePlaceableActorTask(const Task &task, bool push);
  /// This will treat a task removed from the local queue as if it had been
  /// executed and failed. This is done by looping over the task return IDs and
  /// for each ID storing an object that represents a failure in the object
  /// store. When clients retrieve these objects, they will raise
  /// application-level exceptions. State for the task will be cleaned up as if
  /// it were any other task that had been assigned, executed, and removed from
  /// the local queue.
  ///
  /// \param task The task to fail.
  /// \param error_type The type of the error that caused this task to fail.
  /// \return Void.
  void TreatTaskAsFailed(const Task &task, const ErrorType &error_type);
  /// This is similar to TreatTaskAsFailed, but it will only mark the task as
  /// failed if at least one of the task's return values is lost. A return
  /// value is lost if it has been created before, but no longer exists on any
  /// nodes, due to either node failure or eviction.
  ///
  /// \param task The task to potentially fail.
  /// \return Void.
  void TreatTaskAsFailedIfLost(const Task &task);
  /// Handle specified task's submission to the local node manager.
  ///
  /// \param task The task being submitted.
  /// \param uncommitted_lineage The uncommitted lineage of the task.
  /// \param forwarded True if the task has been forwarded from a different
  /// node manager and false if it was submitted by a local worker.
  /// \return Void.
  void SubmitTask(const Task &task, const Lineage &uncommitted_lineage,
                  bool forwarded = false,
                  bool push = false);
  void _SubmitTask(const Task &task, const Lineage &uncommitted_lineage,
                             bool forwarded, bool push);
  /// Assign a task. The task is assumed to not be queued in local_queues_.
  ///
  /// \param task The task in question.
  /// \return true, if tasks was assigned to a worker, false otherwise.
  bool AssignTask(const Task &task);
  void AssignTasksToWorker(const std::vector<Task> &tasks, std::shared_ptr<Worker> worker);
  bool AssignActorTaskBatch(const ActorID &actor_id,
                            const ResourceSet &resource_set,
                            const std::vector<Task> &tasks);
  void FlushTask(const Task &task, const gcs::raylet::TaskTable::WriteCallback &task_callback);
  /// Handle a worker finishing its assigned task.
  ///
  /// \param worker The worker that finished the task.
  /// \return Void.
  void FinishAssignedTasks(Worker &worker, const TaskID &task_id);
  /// Helper function to produce actor table data for a newly created actor.
  ///
  /// \param task The actor creation task that created the actor.
  ActorTableDataT CreateActorTableDataFromCreationTask(const Task &task);
  void SendFlushLineageRequest(const ActorID &actor_id, int64_t actor_version, const ActorID &downstream_actor_id);
  void ProcessFlushLineageRequest(
      const ActorID &upstream_actor_id,
      int64_t upstream_actor_version,
      const ActorID &downstream_actor_id,
      const ClientID &upstream_node_id);
  void SendFlushLineageReply(
      const ActorID &upstream_actor_id,
      const ActorID &downstream_actor_id,
      const ClientID &upstream_node_id);
  void ProcessFlushLineageReply(
      const ActorID &upstream_actor_id,
      int64_t upstream_actor_version,
      const ActorID &downstream_actor_id,
      const ClientID &upstream_node_id);
  void ResumeActorCheckpoint(const ActorID &actor_id,
                             const ActorCheckpointDataT &checkpoint_data,
                             const ActorTableDataT &new_actor_data);
  /// Handle a worker finishing an assigned actor task or actor creation task.
  /// \param worker The worker that finished the task.
  /// \param task The actor task or actor creationt ask.
  /// \return Void.
  void FinishAssignedActorTask(Worker &worker, const Task &task);
  /// Make a placement decision for placeable tasks given the resource_map
  /// provided. This will perform task state transitions and task forwarding.
  ///
  /// \param resource_map A mapping from node manager ID to an estimate of the
  /// resources available to that node manager. Scheduling decisions will only
  /// consider the local node manager and the node managers in the keys of the
  /// resource_map argument.
  /// \return Void.
  void ScheduleTasks(std::unordered_map<ClientID, SchedulingResources> &resource_map);
  /// Handle a task whose return value(s) must be reconstructed.
  ///
  /// \param task_id The relevant task ID.
  /// \return Void.
  void HandleTaskReconstruction(const TaskID &task_id);
  /// Resubmit a task for execution. This is a task that was previously already
  /// submitted to a raylet but which must now be re-executed.
  ///
  /// \param task The task being resubmitted.
  /// \return Void.
  void ResubmitTask(const Task &task);
  /// Attempt to forward a task to a remote different node manager. If this
  /// fails, the task will be resubmit locally.
  ///
  /// \param task The task in question.
  /// \param node_manager_id The ID of the remote node manager.
  /// \return Void.
  void ForwardTaskOrResubmit(const Task &task, const ClientID &node_manager_id);
  /// Forward a task to another node to execute. The task is assumed to not be
  /// queued in local_queues_.
  ///
  /// \param task The task to forward.
  /// \param node_id The ID of the node to forward the task to.
  /// \param on_error Callback on run on non-ok status.
  void ForwardTask(const Task &task, const ClientID &node_id,
                   const std::function<void(const ray::Status &)> &on_error);

  /// Dispatch locally scheduled tasks. This attempts the transition from "scheduled" to
  /// "running" task state.
  ///
  /// This function is called in the following cases:
  ///   (1) A set of new tasks is added to the ready queue.
  ///   (2) New resources are becoming available on the local node.
  ///   (3) A new worker becomes available.
  /// Note in case (1) we only need to look at the new tasks added to the
  /// ready queue, as we know that the old tasks in the ready queue cannot
  /// be scheduled (We checked those tasks last time new resources or
  /// workers became available, and nothing changed since then.) In this case,
  /// tasks_with_resources contains only the newly added tasks to the
  /// ready queue. Otherwise, tasks_with_resources points to entire ready queue.
  /// \param tasks_with_resources Mapping from resource shapes to tasks with
  /// that resource shape.
  void DispatchTasks(
      const std::unordered_map<ResourceSet, ordered_set<TaskID>> &tasks_with_resources);

  /// Handle a task that is blocked. This could be a task assigned to a worker,
  /// an out-of-band task (e.g., a thread created by the application), or a
  /// driver task. This can be triggered when a client starts a get call or a
  /// wait call.
  ///
  /// \param client The client that is executing the blocked task.
  /// \param required_object_ids The IDs that the client is blocked waiting for.
  /// \param current_task_id The task that is blocked.
  /// \param ray_get Whether the task is blocked in a ray.get or a ray.wait.
  /// \return Void.
  void HandleTaskBlocked(const std::shared_ptr<LocalClientConnection> &client,
                         const std::vector<ObjectID> &required_object_ids,
                         const TaskID &current_task_id, bool ray_get);

  /// Handle a task that is unblocked. This could be a task assigned to a
  /// worker, an out-of-band task (e.g., a thread created by the application),
  /// or a driver task. This can be triggered when a client finishes a get call
  /// or a wait call. The given task must be blocked, via a previous call to
  /// HandleTaskBlocked.
  ///
  /// \param client The client that is executing the unblocked task.
  /// \param current_task_id The task that is unblocked.
  /// \return Void.
  void HandleTaskUnblocked(const std::shared_ptr<LocalClientConnection> &client,
                           const TaskID &current_task_id);

  /// Kill a worker.
  ///
  /// \param worker The worker to kill.
  /// \return Void.
  void KillWorker(std::shared_ptr<Worker> worker);

  /// The callback for handling an actor state transition (e.g., from ALIVE to
  /// DEAD), whether as a notification from the actor table or as a handler for
  /// a local actor's state transition. This method is idempotent and will ignore
  /// old state transition.
  ///
  /// \param actor_id The actor ID of the actor whose state was updated.
  /// \param actor_registration The ActorRegistration object that represents actor's
  /// new state.
  /// \return Void.
  void HandleActorStateTransition(const ActorID &actor_id,
                                  ActorRegistration &&actor_registration);

  void SubmitWaitingForActorCreationTasks(const ActorID &actor_id);

  /// Publish an actor's state transition to all other nodes.
  ///
  /// \param actor_id The actor ID of the actor whose state was updated.
  /// \param data Data to publish.
  /// \param failure_callback An optional callback to call if the publish is
  /// unsuccessful.
  void PublishActorStateTransition(
      const ActorID &actor_id, const ActorTableDataT &data,
      const ray::gcs::ActorTable::WriteCallback &failure_callback);

  /// When a driver dies, loop over all of the queued tasks for that driver and
  /// treat them as failed.
  ///
  /// \param driver_id The driver that died.
  /// \return Void.
  void CleanUpTasksForDeadDriver(const DriverID &driver_id);

  /// Handle an object becoming local. This updates any local accounting, but
  /// does not write to any global accounting in the GCS.
  ///
  /// \param object_id The object that is locally available.
  /// \return Void.
  void HandleObjectLocal(const ObjectID &object_id);
  /// Handle an object that is no longer local. This updates any local
  /// accounting, but does not write to any global accounting in the GCS.
  ///
  /// \param object_id The object that has been evicted locally.
  /// \return Void.
  void HandleObjectMissing(const ObjectID &object_id);

  /// Handles updates to driver table.
  ///
  /// \param id An unused value. TODO(rkn): Should this be removed?
  /// \param driver_data Data associated with a driver table event.
  /// \return Void.
  void HandleDriverTableUpdate(const ClientID &id,
                               const std::vector<DriverTableDataT> &driver_data);

  /// Check if certain invariants associated with the task dependency manager
  /// and the local queues are satisfied. This is only used for debugging
  /// purposes.
  ///
  /// \return True if the invariants are satisfied and false otherwise.
  bool CheckDependencyManagerInvariant() const;

  /// Process client message of RegisterClientRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessRegisterClientRequestMessage(
      const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data);

  /// Process client message of GetTask
  ///
  /// \param client The client that sent the message.
  /// \return Void.
  void ProcessGetTaskMessage(const std::shared_ptr<LocalClientConnection> &client);

  /// Handle a client that has disconnected. This can be called multiple times
  /// on the same client because this is triggered both when a client
  /// disconnects and when the node manager fails to write a message to the
  /// client.
  ///
  /// \param client The client that sent the message.
  /// \param intentional_disconnect Wether the client was intentionally disconnected.
  /// \return Void.
  void ProcessDisconnectClientMessage(
      const std::shared_ptr<LocalClientConnection> &client,
      bool intentional_disconnect = false);

  /// Process client message of SubmitTask
  ///
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessSubmitTaskMessage(const uint8_t *message_data);

  /// Process client message of FetchOrReconstruct
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessFetchOrReconstructMessage(
      const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data);

  /// Process client message of WaitRequest
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessWaitRequestMessage(const std::shared_ptr<LocalClientConnection> &client,
                                 const uint8_t *message_data);

  /// Process client message of PushErrorRequest
  ///
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void ProcessPushErrorRequestMessage(const uint8_t *message_data);

  /// Process client message of PrepareActorCheckpointRequest.
  ///
  /// \param client The client that sent the message.
  /// \param message_data A pointer to the message data.
  void ProcessPrepareActorCheckpointRequest(
      const std::shared_ptr<LocalClientConnection> &client, const uint8_t *message_data);

  /// Process client message of NotifyActorResumedFromCheckpoint.
  ///
  /// \param message_data A pointer to the message data.
  void ProcessNotifyActorResumedFromCheckpoint(const uint8_t *message_data);

  /// Update actor frontier when a task finishes.
  /// If the task is an actor creation task and the actor was resumed from a checkpoint,
  /// restore the frontier from the checkpoint. Otherwise, just extend actor frontier.
  ///
  /// \param task The task that just finished.
  void UpdateActorFrontier(const Task &task);

  /// Handle the case where an actor is disconnected, determine whether this
  /// actor needs to be reconstructed and then update actor table.
  /// This function needs to be called either when actor process dies or when
  /// a node dies.
  ///
  /// \param actor_id Id of this actor.
  /// \param was_local Whether the disconnected was on this local node.
  /// \param intentional_disconnect Wether the client was intentionally disconnected.
  /// \return Void.
  void HandleDisconnectedActor(const ActorID &actor_id, bool was_local,
                               bool intentional_disconnect);

  /// connect to a remote node manager.
  ///
  /// \param client_id The client ID for the remote node manager.
  /// \param client_address The IP address for the remote node manager.
  /// \param client_port The listening port for the remote node manager.
  /// \return True if the connect succeeds.
  ray::Status ConnectRemoteNodeManager(const ClientID &client_id,
                                       const std::string &client_address,
                                       int32_t client_port);

  // GCS client ID for this node.
  ClientID client_id_;
  boost::asio::io_service &io_service_;
  ObjectManager &object_manager_;
  /// A Plasma object store client. This is used exclusively for creating new
  /// objects in the object store (e.g., for actor tasks that can't be run
  /// because the actor died).
  plasma::PlasmaClient store_client_;
  /// A client connection to the GCS.
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_;
  /// The object table. This is shared with the object manager.
  std::shared_ptr<ObjectDirectoryInterface> object_directory_;
  /// The timer used to send heartbeats.
  boost::asio::steady_timer heartbeat_timer_;
  /// The period used for the heartbeat timer.
  std::chrono::milliseconds heartbeat_period_;
  /// The period between debug state dumps.
  int64_t debug_dump_period_;
  /// The path to the ray temp dir.
  std::string temp_dir_;
  int gcs_delay_ms_;
  bool use_gcs_only_;
  /// The timer used to get profiling information from the object manager and
  /// push it to the GCS.
  boost::asio::steady_timer object_manager_profile_timer_;
  /// The time that the last heartbeat was sent at. Used to make sure we are
  /// keeping up with heartbeats.
  uint64_t last_heartbeat_at_ms_;
  /// The time that the last debug string was logged to the console.
  uint64_t last_debug_dump_at_ms_;
  /// Initial node manager configuration.
  const NodeManagerConfig initial_config_;
  /// The resources (and specific resource IDs) that are currently available.
  ResourceIdSet local_available_resources_;
  std::unordered_map<ClientID, SchedulingResources> cluster_resource_map_;
  /// A pool of workers.
  WorkerPool worker_pool_;
  /// A set of queues to maintain tasks.
  SchedulingQueue local_queues_;
  /// The scheduling policy in effect for this local scheduler.
  SchedulingPolicy scheduling_policy_;
  /// The reconstruction policy for deciding when to re-execute a task.
  ReconstructionPolicy reconstruction_policy_;
  /// A manager to make waiting tasks's missing object dependencies available.
  TaskDependencyManager task_dependency_manager_;
  /// The lineage cache for the GCS object and task tables.
  LineageCache lineage_cache_;
  std::vector<ClientID> remote_clients_;
  std::unordered_map<ClientID, std::shared_ptr<TcpServerConnection>>
      remote_server_connections_;
  /// A mapping from actor ID to registration information about that actor
  /// (including which node manager owns it).
  std::unordered_map<ActorID, ActorRegistration> actor_registry_;

  /// This map stores actor ID to the ID of the checkpoint that will be used to
  /// restore the actor.
  std::unordered_map<ActorID, ActorCheckpointID> checkpoint_id_to_restore_;

  struct PendingFlushAllRequest {
    PendingFlushAllRequest(
    const ActorID &upstream_actor,
    const ActorID &downstream_actor,
    const ClientID &upstream_node)
      : upstream_actor_id(upstream_actor),
        downstream_actor_id(downstream_actor),
        upstream_node_id(upstream_node) {}
    const ActorID upstream_actor_id;
    const ActorID downstream_actor_id;
    const ClientID upstream_node_id;
  };

  // A list of {upstream actor ID, downstream actor ID} FlushLineageRequests
  // whose FlushAll calls are pending.
  std::vector<PendingFlushAllRequest> pending_flush_requests_;
  // For downstream actors whose locations we don't know yet. The value is the
  // local upstream actor that is recovering and its version number. Once the
  // location for the downstream actor is found, we will send a
  // FlushLineageRequest on behalf of the upstream actor.
  std::unordered_map<ActorID, std::vector<std::pair<ActorID, int64_t>>> pending_downstream_actors_;
  // The upstream actors that are waiting for us to recover. The key is the
  // local actor. The value is all of the upstream actors that sent a
  // FlushLineageRequest to the local actor, paired with their node locations.
  // Once we receive all FlushLineageReply messages for the local actor, we
  // will send a FlushLineageReply message to the corresponding upstream actors
  // in this map.
  std::unordered_map<ActorID, std::vector<std::pair<ActorID, ClientID>>> recovering_upstream_actors_;
};

}  // namespace raylet

}  // end namespace ray

#endif  // RAY_RAYLET_NODE_MANAGER_H
