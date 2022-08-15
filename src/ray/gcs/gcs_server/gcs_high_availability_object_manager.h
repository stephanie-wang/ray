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

#pragma once

#include "ray/common/runtime_env_manager.h"
#include "ray/gcs/gcs_server/gcs_function_manager.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

class GcsHighAvailabilityObjectManager : public rpc::HighAvailabilityObjectHandler {
 public:
  explicit GcsHighAvailabilityObjectManager() {}

  void HandleGetHighAvailabilityObject(const rpc::GetHighAvailabilityObjectRequest &request,
                            rpc::GetHighAvailabilityObjectReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  void HandlePutHighAvailabilityObject(const rpc::PutHighAvailabilityObjectRequest &request,
                            rpc::PutHighAvailabilityObjectReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  void PutHighAvailabilityObject(const ObjectID &object_id,
      const ActorID &actor_id,
      const rpc::Address &owner_address);

 private:
  absl::flat_hash_map<ObjectID, std::pair<ActorID, rpc::Address>> objects_;
};

}  // namespace gcs
}  // namespace ray
