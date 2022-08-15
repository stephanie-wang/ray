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
  ObjectID obj_id = ObjectID::FromBinary(request.object_id());
  ActorID actor_id = ActorID::FromBinary(request.data().actor_id());
  PutHighAvailabilityObject(obj_id, actor_id, request.data().owner_address());
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsHighAvailabilityObjectManager::PutHighAvailabilityObject(const ObjectID &object_id,
    const ActorID &actor_id,
    const rpc::Address &owner_address) {
  RAY_LOG(DEBUG) << "Object ID " << object_id << " owned by actor " << actor_id << " " << owner_address.actor_name();
  RAY_CHECK(objects_.emplace(object_id, std::make_pair(actor_id, owner_address)).second);
}


}  // namespace gcs
}  // namespace ray
