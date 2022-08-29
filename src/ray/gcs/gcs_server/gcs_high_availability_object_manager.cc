// Copyright 2017 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_high_availability_object_manager.h"

#include "ray/gcs/pb_util.h"

namespace ray {
namespace gcs {

void GcsHighAvailabilityObjectManager::HandleGetHighAvailabilityObject(const rpc::GetHighAvailabilityObjectRequest &request,
                          rpc::GetHighAvailabilityObjectReply *reply,
                          rpc::SendReplyCallback send_reply_callback) {
  ObjectID obj_id = ObjectID::FromBinary(request.object_id());
  RAY_LOG(DEBUG) << "GET " << obj_id;
  reply->mutable_data()->set_object_id(obj_id.Binary());
  auto it = objects_.find(obj_id);
  if (it != objects_.end()) {
    reply->mutable_data()->set_actor_id(it->second.first.Binary());
    reply->mutable_data()->mutable_owner_address()->CopyFrom(it->second.second);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsHighAvailabilityObjectManager::HandlePutHighAvailabilityObject(const rpc::PutHighAvailabilityObjectRequest &request,
                          rpc::PutHighAvailabilityObjectReply *reply,
                          rpc::SendReplyCallback send_reply_callback) {
  //ObjectID obj_id = ObjectID::FromBinary(request.object_id());
  //ActorID actor_id = ActorID::FromBinary(request.data().actor_id());
  //PutHighAvailabilityObjects(obj_id, actor_id, request.data().owner_address());
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsHighAvailabilityObjectManager::PutHighAvailabilityObjects(const std::vector<ObjectID> &object_ids,
      const absl::flat_hash_map<TaskID, TaskSpecification> &lineage,
      const ActorID &actor_id,
      const rpc::Address &owner_address) {
  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG) << "Object ID " << object_id << " owned by actor " << actor_id << " " << owner_address.actor_name();
    RAY_CHECK(objects_.emplace(object_id, std::make_pair(actor_id, owner_address)).second);
  }

  for (const auto &task : lineage) {
    lineage_.emplace(task.first, task.second);
  }
}

void GcsHighAvailabilityObjectManager::GetLineage(const ObjectID &object_id,
                absl::flat_hash_map<TaskID, TaskSpecification> *lineage) const {
  const TaskID &task_id = object_id.TaskId();
  if (lineage->count(task_id)) {
    return;
  }
  const auto it = lineage_.find(task_id);
  if (it == lineage_.end()) {
    return;
  }
  RAY_CHECK(lineage->emplace(task_id, it->second).second);

  // If the task can no longer be retried, decrement the lineage ref count
  // for each of the task's args.
  for (size_t i = 0; i < it->second.NumArgs(); i++) {
    if (it->second.ArgByRef(i)) {
      GetLineage(it->second.ArgId(i), lineage);
    } else {
      const auto &inlined_refs = it->second.ArgInlinedRefs(i);
      for (const auto &inlined_ref : inlined_refs) {
        GetLineage(ObjectID::FromBinary(inlined_ref.object_id()), lineage);
      }
    }
  }
}

}  // namespace gcs
}  // namespace ray
