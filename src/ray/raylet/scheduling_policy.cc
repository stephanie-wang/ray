#include <algorithm>
#include <chrono>
#include <random>

#include "scheduling_policy.h"

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

SchedulingPolicy::SchedulingPolicy(const SchedulingQueue &scheduling_queue)
    : scheduling_queue_(scheduling_queue),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

ClientID SchedulingPolicy::GetPlacementByGroup(
    const GroupID &group_id,
    const GroupID &group_dependency,
    const std::unordered_map<ClientID, SchedulingResources> &cluster_resources) const {
  const auto group_dependency_schedule = group_schedule_.find(group_dependency);
  const auto group_schedule = group_schedule_.find(group_id);

  if (group_dependency_schedule != group_schedule_.end()) {
    for (const auto &schedule : group_dependency_schedule->second) {
      const ClientID &node_id = schedule.first;
      // Get the number of CPUs available on this node.
      double num_cpus = 0;
      const auto node_resources = cluster_resources.find(node_id);
      if (node_resources != cluster_resources.end()) {
        num_cpus = node_resources->second.GetTotalResources().GetNumCpus();
      }
      int64_t num_tasks = 0;
      if (group_schedule != group_schedule_.end()) {
        auto it = group_schedule->second.find(node_id);
        if (it != group_schedule->second.end()) {
          num_tasks = it->second;
        }
      }
      if (num_tasks < num_cpus) {
        return node_id;
      }
    }
  }

  if (group_schedule != group_schedule_.end()) {
    for (const auto &schedule : group_schedule->second) {
      const ClientID &node_id = schedule.first;
      // Get the number of CPUs available on this node.
      double num_cpus = 0;
      const auto node_resources = cluster_resources.find(node_id);
      if (node_resources != cluster_resources.end()) {
        num_cpus = node_resources->second.GetTotalResources().GetNumCpus();
      }
      if (schedule.second < num_cpus) {
        return node_id;
      }
    }
  }

  // There is no more room left, so the task should be placed according to
  // current global load.
  return ClientID::nil();
}

ClientID SchedulingPolicy::GetPlacementByAvailability(
    const std::unordered_map<ClientID, SchedulingResources> &cluster_resources) const {
  // Get the client with the maximum availability in terms of CPU resources.
  ClientID client_id = ClientID::nil();
  int64_t max_availability = 0;
  for (const auto &resources : cluster_resources) {
    const auto &node_resources = resources.second;
    // The node's availability is equal to its available resources - the
    // current load in its queue.
    int64_t availability = node_resources.GetAvailableResources().GetNumCpus() -
                           node_resources.GetLoadResources().GetNumCpus();
    if (availability > max_availability || client_id.is_nil()) {
      client_id = resources.first;
      max_availability = availability;
    }
  }
  return client_id;
}

std::unordered_map<TaskID, ClientID> SchedulingPolicy::ScheduleByGroup(
    std::unordered_map<ClientID, SchedulingResources> &cluster_resources,
    const ClientID &local_client_id) {
  // The policy decision to be returned.
  std::unordered_map<TaskID, ClientID> decision;

  // Iterate over running tasks, get their resource demand and try to schedule.
  for (const auto &t : scheduling_queue_.GetPlaceableTasks()) {
    // Get task's resource demand
    const auto &spec = t.GetTaskSpecification();
    const auto &resource_demand = spec.GetRequiredPlacementResources();
    const TaskID &task_id = spec.TaskId();

    ClientID client_id = ClientID::nil();
    if (spec.GroupDependency().is_nil()) {
      // Pick the task's placement according to its individual data
      // dependencies. NOTE: This only considers the first dependency,
      // according to the order in the task spec.
      const auto &dependencies = t.GetDependencies();
      if (!dependencies.empty()) {
        // Colocate the task with the first of its data dependencies.
        const auto &dependency = dependencies.front();
        const auto it = task_schedule_.find(ComputeTaskId(dependency));
        if (it != task_schedule_.end()) {
          client_id = it->second;
        }
      }
    } else {
      RAY_CHECK(!spec.GroupId().is_nil());
      // Try to pack the group onto the same nodes that the dependency is on,
      // resources allowing.
      client_id = GetPlacementByGroup(spec.GroupId(), spec.GroupDependency(), cluster_resources);
    }
    // Placement by data dependency is no longer efficient, so just pick the
    // node with the greatest availability.
    if (client_id.is_nil()) {
      client_id = GetPlacementByAvailability(cluster_resources);
    }

    // Update dst_client_id's load to keep track of remote task load until
    // the next heartbeat.
    ResourceSet new_load(cluster_resources[client_id].GetLoadResources());
    new_load.AddResources(resource_demand);
    cluster_resources[client_id].SetLoadResources(std::move(new_load));

    // Remember the scheduling decision.
    task_schedule_[task_id] = client_id;
    if (!spec.GroupId().is_nil()) {
      group_schedule_[spec.GroupId()][client_id]++;
      groups_[spec.GroupId()].push_back(task_id);
    }
    decision[task_id] = client_id;
  }
  return decision;
}

void SchedulingPolicy::FreeGroup(const GroupID &group_id) {
  RAY_LOG(INFO) << "Freeing group " << group_id;
  for (const auto &task_id : groups_[group_id]) {
    RAY_CHECK(task_schedule_.erase(task_id));
  }
  RAY_CHECK(groups_.erase(group_id));
  RAY_CHECK(group_schedule_.erase(group_id));
}

std::unordered_map<TaskID, ClientID> SchedulingPolicy::Schedule(
    std::unordered_map<ClientID, SchedulingResources> &cluster_resources,
    const ClientID &local_client_id) {
  // The policy decision to be returned.
  std::unordered_map<TaskID, ClientID> decision;
  // TODO(atumanov): protect DEBUG code blocks with ifdef DEBUG
  RAY_LOG(DEBUG) << "[Schedule] cluster resource map: ";

#ifndef NDEBUG
  for (const auto &client_resource_pair : cluster_resources) {
    // pair = ClientID, SchedulingResources
    const ClientID &client_id = client_resource_pair.first;
    const SchedulingResources &resources = client_resource_pair.second;
    RAY_LOG(DEBUG) << "client_id: " << client_id << " "
                   << resources.GetAvailableResources().ToString();
  }
#endif

  // We expect all placeable tasks to be placed on exit from this policy method.
  RAY_CHECK(scheduling_queue_.GetPlaceableTasks().size() <= 1);
  // Iterate over running tasks, get their resource demand and try to schedule.
  for (const auto &t : scheduling_queue_.GetPlaceableTasks()) {
    // Get task's resource demand
    const auto &spec = t.GetTaskSpecification();
    const auto &resource_demand = spec.GetRequiredPlacementResources();
    const TaskID &task_id = spec.TaskId();

    // TODO(atumanov): try to place tasks locally first.
    // Construct a set of viable node candidates and randomly pick between them.
    // Get all the client id keys and randomly pick.
    std::vector<ClientID> client_keys;
    for (const auto &client_resource_pair : cluster_resources) {
      // pair = ClientID, SchedulingResources
      ClientID node_client_id = client_resource_pair.first;
      const auto &node_resources = client_resource_pair.second;
      ResourceSet available_node_resources =
          ResourceSet(node_resources.GetAvailableResources());
      available_node_resources.SubtractResourcesStrict(node_resources.GetLoadResources());
      RAY_LOG(DEBUG) << "client_id " << node_client_id
                     << " avail: " << node_resources.GetAvailableResources().ToString()
                     << " load: " << node_resources.GetLoadResources().ToString()
                     << " avail-load: " << available_node_resources.ToString();

      if (resource_demand.IsSubset(available_node_resources)) {
        // This node is a feasible candidate.
        client_keys.push_back(node_client_id);
      }
    }

    if (!client_keys.empty()) {
      // Choose index at random.
      // Initialize a uniform integer distribution over the key space.
      // TODO(atumanov): change uniform random to discrete, weighted by resource capacity.
      std::uniform_int_distribution<int> distribution(0, client_keys.size() - 1);
      int client_key_index = distribution(gen_);
      const ClientID &dst_client_id = client_keys[client_key_index];
      decision[task_id] = dst_client_id;
      // Update dst_client_id's load to keep track of remote task load until
      // the next heartbeat.
      ResourceSet new_load(cluster_resources[dst_client_id].GetLoadResources());
      new_load.AddResources(resource_demand);
      cluster_resources[dst_client_id].SetLoadResources(std::move(new_load));
    } else {
      // If the task doesn't fit, place randomly subject to hard constraints.
      for (const auto &client_resource_pair2 : cluster_resources) {
        // pair = ClientID, SchedulingResources
        ClientID node_client_id = client_resource_pair2.first;
        const auto &node_resources = client_resource_pair2.second;
        if (resource_demand.IsSubset(node_resources.GetTotalResources())) {
          // This node is a feasible candidate.
          client_keys.push_back(node_client_id);
        }
      }
      // client candidate list constructed, pick randomly.
      if (!client_keys.empty()) {
        // Choose index at random.
        // Initialize a uniform integer distribution over the key space.
        // TODO(atumanov): change uniform random to discrete, weighted by resource
        // capacity.
        std::uniform_int_distribution<int> distribution(0, client_keys.size() - 1);
        int client_key_index = distribution(gen_);
        const ClientID &dst_client_id = client_keys[client_key_index];
        decision[task_id] = dst_client_id;
        // Update dst_client_id's load to keep track of remote task load until
        // the next heartbeat.
        ResourceSet new_load(cluster_resources[dst_client_id].GetLoadResources());
        new_load.AddResources(resource_demand);
        cluster_resources[dst_client_id].SetLoadResources(std::move(new_load));
      } else {
        // There are no nodes that can feasibly execute this task. The task remains
        // placeable until cluster capacity becomes available.
        // TODO(rkn): Propagate a warning to the user.
        RAY_LOG(INFO) << "The task with ID " << task_id << " requires "
                      << spec.GetRequiredResources().ToString() << " for execution and "
                      << spec.GetRequiredPlacementResources().ToString()
                      << " for placement, but no nodes have the necessary resources. "
                      << "Check the client table to view node resources.";
      }
    }
  }

  return decision;
}

std::vector<TaskID> SchedulingPolicy::SpillOver(
    SchedulingResources &remote_scheduling_resources) const {
  // The policy decision to be returned.
  std::vector<TaskID> decision;

  ResourceSet new_load(remote_scheduling_resources.GetLoadResources());

  // Check if we can accommodate infeasible tasks.
  for (const auto &task : scheduling_queue_.GetInfeasibleTasks()) {
    const auto &spec = task.GetTaskSpecification();
    const auto &placement_resources = spec.GetRequiredPlacementResources();
    if (placement_resources.IsSubset(remote_scheduling_resources.GetTotalResources())) {
      decision.push_back(spec.TaskId());
      new_load.AddResources(spec.GetRequiredResources());
    }
  }

  // Try to accommodate up to a single ready task.
  for (const auto &task : scheduling_queue_.GetReadyTasks()) {
    const auto &spec = task.GetTaskSpecification();
    if (!spec.IsActorTask()) {
      // Make sure the node has enough available resources to prevent forwarding cycles.
      if (spec.GetRequiredPlacementResources().IsSubset(
              remote_scheduling_resources.GetAvailableResources())) {
        decision.push_back(spec.TaskId());
        new_load.AddResources(spec.GetRequiredResources());
        break;
      }
    }
  }
  remote_scheduling_resources.SetLoadResources(std::move(new_load));

  return decision;
}

SchedulingPolicy::~SchedulingPolicy() {}

}  // namespace raylet

}  // namespace ray
