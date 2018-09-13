#include "scheduling_policy.h"

#include <chrono>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

SchedulingPolicy::SchedulingPolicy(const SchedulingQueue &scheduling_queue)
    : scheduling_queue_(scheduling_queue),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

std::unordered_map<TaskID, ClientID> SchedulingPolicy::Schedule(
    const std::unordered_map<ClientID, SchedulingResources> &cluster_resources,
    const ClientID &local_client_id, const std::vector<ClientID> &others,
    const SchedulingBuffer &buffer) {
  // The policy decision to be returned.
  std::unordered_map<TaskID, ClientID> decision;
  // TODO(atumanov): protect DEBUG code blocks with ifdef DEBUG
  //RAY_LOG(DEBUG) << "[Schedule] cluster resource map: ";
  //for (const auto &client_resource_pair : cluster_resources) {
  //  // pair = ClientID, SchedulingResources
  //  const ClientID &client_id = client_resource_pair.first;
  //  const SchedulingResources &resources = client_resource_pair.second;
  //  RAY_LOG(DEBUG) << "client_id: " << client_id << " "
  //                 << resources.GetAvailableResources().ToString();
  //}

  // Iterate over running tasks, get their resource demand and try to schedule.
  for (const auto &t : scheduling_queue_.GetPlaceableTasks()) {
    // Get task's resource demand
    auto resource_demand = t.GetTaskSpecification().GetRequiredResources();

    // TODO(swang): Hack to schedule reconstructed tasks that require
    // custom resources that are no longer available.
    if (t.GetTaskExecutionSpec().NumExecutions() > 0) {
      resource_demand = resource_demand.GetCpuResources();
    }

    const TaskID &task_id = t.GetTaskSpecification().TaskId();
    //RAY_LOG(DEBUG) << "[SchedulingPolicy]: task=" << task_id
    //               << " numforwards=" << t.GetTaskExecutionSpec().NumForwards()
    //               << " resources="
    //               << t.GetTaskSpecification().GetRequiredResources().ToString();
    // TODO(atumanov): replace the simple spillback policy with exponential backoff based
    // policy.
    //if (t.GetTaskExecutionSpecReadonly().NumForwards() >= 1) {
    //  decision[task_id] = local_client_id;
    //  continue;
    //}
    // Construct a set of viable node candidates and randomly pick between them.
    // Get all the client id keys and randomly pick.
    std::vector<ClientID> client_keys;
    for (const auto &client_resource_pair : cluster_resources) {
      // pair = ClientID, SchedulingResources
      ClientID node_client_id = client_resource_pair.first;
      const auto &node_resources = client_resource_pair.second;
      //RAY_LOG(DEBUG) << "client_id " << node_client_id << " resources: "
      //               << node_resources.GetAvailableResources().ToString();
      if (resource_demand.IsSubset(node_resources.GetTotalResources())) {
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
      decision[task_id] = client_keys[client_key_index];
      //RAY_LOG(DEBUG) << "[SchedulingPolicy] idx=" << client_key_index << " " << task_id
      //               << " --> " << client_keys[client_key_index];
    } else {
      ClientID node_decision = ClientID::nil();
      for (int i = 0; i < t.GetTaskSpecification().NumArgs(); ++i) {
        int count = t.GetTaskSpecification().ArgIdCount(i);
        for (int j = 0; j < count; j++) {
          ObjectID argument_id = t.GetTaskSpecification().ArgId(i, j);
          const ClientID scheduled_node_id = buffer.GetDecision(argument_id);
          if (!scheduled_node_id.is_nil()) {
            node_decision = scheduled_node_id;
          }
        }
      }

      if (node_decision.is_nil() || cluster_resources.count(node_decision) == 0) {
        // TODO(swang): Hack to schedule reconstructed tasks that require
        // custom resources that are no longer available.
        // There are no nodes that can feasibily execute this task. TODO(rkn): Propagate a
        // warning to the user.
        //RAY_LOG(DEBUG) << "This task requires "
        //                 << resource_demand.ToString()
        //                 << ", but no nodes have the necessary resources.";
        resource_demand = resource_demand.GetCpuResources();
        for (const auto &client_resource_pair : cluster_resources) {
          // pair = ClientID, SchedulingResources
          ClientID node_client_id = client_resource_pair.first;
          const auto &node_resources = client_resource_pair.second;
          //RAY_LOG(DEBUG) << "client_id " << node_client_id << " resources: "
          //               << node_resources.GetAvailableResources().ToString();
          if (resource_demand.IsSubset(node_resources.GetTotalResources())) {
            // This node is a feasible candidate.
            client_keys.push_back(node_client_id);
          }
        }
        RAY_CHECK(!client_keys.empty());
        // Choose index at random.
        // Initialize a uniform integer distribution over the key space.
        // TODO(atumanov): change uniform random to discrete, weighted by resource capacity.
        std::uniform_int_distribution<int> distribution(0, client_keys.size() - 1);
        int client_key_index = distribution(gen_);
        node_decision = client_keys[client_key_index];
        //RAY_LOG(DEBUG) << "[SchedulingPolicy] idx=" << client_key_index << " " << task_id
        //               << " --> " << client_keys[client_key_index];

      }

      decision[task_id] = node_decision;
      RAY_LOG(INFO) << "No feasible node, giving task " << task_id << " to " << node_decision;
    }
  }
  return decision;
}

SchedulingPolicy::~SchedulingPolicy() {}

}  // namespace raylet

}  // namespace ray
