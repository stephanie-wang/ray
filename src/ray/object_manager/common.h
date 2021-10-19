// Copyright 2020-2021 The Ray Authors.
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

#pragma once

#include <boost/asio.hpp>
#include <functional>

#include "ray/common/id.h"
#include "ray/common/task/task_priority.h"
#include "src/ray/protobuf/common.pb.h"
#include "ray/common/status.h"

namespace ray {

/// A callback to asynchronously spill objects when space is needed.
/// It spills enough objects to saturate all spill IO workers.
using SpillObjectsCallback = std::function<bool()>;

/// A callback to call when space has been released.
using SpaceReleasedCallback = std::function<void()>;

/// A callback to call when a spilled object needs to be returned to the object store.
using RestoreSpilledObjectCallback = std::function<void(
    const ObjectID &, const Priority &priority, const std::string &, std::function<void(const ray::Status &)>)>;

using AsyncPreemptCallback = std::function<void(const ObjectID &object_id, const ray::Priority &priority,
    int64_t data_size, const std::vector<ObjectID> &task_deps)>;

/// A struct that includes info about the object.
struct ObjectInfo {
  ObjectID object_id;
  int64_t data_size;
  int64_t metadata_size;
  /// Owner's raylet ID.
  NodeID owner_raylet_id;
  /// Owner's IP address.
  std::string owner_ip_address;
  /// Owner's port.
  int owner_port;
  /// Owner's worker ID.
  WorkerID owner_worker_id;
  Priority priority;

  int64_t GetObjectSize() const { return data_size + metadata_size; }

  bool operator==(const ObjectInfo &other) const {
    return ((object_id == other.object_id) && (data_size == other.data_size) &&
            (metadata_size == other.metadata_size) &&
            (owner_raylet_id == other.owner_raylet_id) &&
            (owner_ip_address == other.owner_ip_address) &&
            (owner_port == other.owner_port) &&
            (owner_worker_id == other.owner_worker_id));
  }
};

// A callback to call when an object is added to the shared memory store.
using AddObjectCallback = std::function<void(const ObjectInfo &)>;

// A callback to call when an object is removed from the shared memory store.
using DeleteObjectCallback = std::function<void(const ObjectID &)>;

using PreemptObjectCallback = std::function<void(const rpc::ObjectReference &object_ref, std::function<void(bool)>)>;

using ScheduleRemoteMemoryCallback = std::function<NodeID(int64_t space_needed)>;

using TaskQueueInfoCallback = std::function<void(
    const NodeID &remote_node_id,
    bool higher_priority_ready_task,
    bool higher_priority_running_task
    )>;

// Returns <higher_priority_ready_task, higher_priority_running_task>.
using CheckTaskQueuesCallback = std::function<void(int64_t space_needed, const Priority &priority, TaskQueueInfoCallback callback)>;

}  // namespace ray
