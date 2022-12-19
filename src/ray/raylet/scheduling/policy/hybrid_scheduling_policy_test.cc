// Copyright 2021 The Ray Authors.
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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/raylet/scheduling/policy/composite_scheduling_policy.h"

namespace ray {

namespace raylet_scheduling_policy {

using ::testing::_;
using namespace ray::raylet;

NodeResources CreateNodeResources(double available_cpu,
                                  double total_cpu,
                                  double available_memory,
                                  double total_memory,
                                  double available_gpu,
                                  double total_gpu) {
  NodeResources resources;
  resources.available.Set(ResourceID::CPU(), available_cpu)
      .Set(ResourceID::Memory(), available_memory)
      .Set(ResourceID::GPU(), available_gpu);
  resources.total.Set(ResourceID::CPU(), total_cpu)
      .Set(ResourceID::Memory(), total_memory)
      .Set(ResourceID::GPU(), total_gpu);
  return resources;
}

class HybridSchedulingPolicyTest : public ::testing::Test {
 public:
  scheduling::NodeID local_node = scheduling::NodeID(0);
  scheduling::NodeID remote_node = scheduling::NodeID(1);
  scheduling::NodeID remote_node_2 = scheduling::NodeID(2);
  scheduling::NodeID remote_node_3 = scheduling::NodeID(3);
  absl::flat_hash_map<scheduling::NodeID, Node> nodes;

  SchedulingOptions HybridOptions(
      float spread,
      bool avoid_local_node,
      bool require_node_available,
      bool avoid_gpu_nodes = RayConfig::instance().scheduler_avoid_gpu_nodes(),
      int schedule_top_k_absolute = 1,
      float scheduler_top_k_fraction = 0.1,
      int64_t max_pending_lease_requests_per_scheduling_category = 5) {
    return SchedulingOptions(SchedulingType::HYBRID,
                             RayConfig::instance().scheduler_spread_threshold(),
                             avoid_local_node,
                             require_node_available,
                             avoid_gpu_nodes,
                             /*max_cpu_fraction_per_node*/ 1.0,
                             /*scheduling_context*/ nullptr,
                             schedule_top_k_absolute,
                             scheduler_top_k_fraction,
                             max_pending_lease_requests_per_scheduling_category);
  }

  ClusterResourceManager MockClusterResourceManager(
      const absl::flat_hash_map<scheduling::NodeID, Node> &nodes) {
    ClusterResourceManager cluster_resource_manager;
    cluster_resource_manager.nodes_ = nodes;
    return cluster_resource_manager;
  }
};

TEST_F(HybridSchedulingPolicyTest, NumNodesToSelect) {
  HybridSchedulingPolicy policy{local_node, {}, [](scheduling::NodeID) { return true; }};
  int32_t schedule_top_k_absolute = 1;
  for (int32_t schedule_top_k_absolute = 1; schedule_top_k_absolute <= 10; schedule_top_k_absolute++) {
    EXPECT_EQ(schedule_top_k_absolute, NumNodesToSelect(schedule_top_k_absolute, 0, 10, 10));
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

}  // namespace raylet_scheduling_policy

}  // namespace ray
