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

#include "ray/object_manager/plasma/create_request_queue.h"

#include <stdlib.h>

#include <memory>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/plasma_generated.h"
#include "ray/util/util.h"

namespace plasma {

uint64_t CreateRequestQueue::AddRequest(const ray::TaskKey &key, const ObjectID &object_id,
                                        const std::shared_ptr<ClientInterface> &client,
                                        const CreateObjectCallback &create_callback,
                                        size_t object_size) {
  auto req_id = next_req_id_++;
  index_[req_id] = key;
  fulfilled_requests_[req_id] = nullptr;
  queue_.emplace(
      key,
      new CreateRequest(object_id, req_id, client, create_callback, object_size));
  num_bytes_pending_ += object_size;
  return req_id;
}

bool CreateRequestQueue::GetRequestResult(uint64_t req_id, PlasmaObject *result,
                                          PlasmaError *error) {
  auto it = fulfilled_requests_.find(req_id);
  if (it == fulfilled_requests_.end()) {
    RAY_LOG(ERROR)
        << "Object store client requested the result of a previous request to create an "
           "object, but the result has already been returned to the client. This client "
           "may hang because the creation request cannot be fulfilled.";
    *error = PlasmaError::UnexpectedError;
    return true;
  }

  if (!it->second) {
    return false;
  }

  *result = it->second->result;
  *error = it->second->error;
  fulfilled_requests_.erase(it);
  return true;
}

std::pair<PlasmaObject, PlasmaError> CreateRequestQueue::TryRequestImmediately(
    const ray::TaskKey &key,
    const ObjectID &object_id, const std::shared_ptr<ClientInterface> &client,
    const CreateObjectCallback &create_callback, size_t object_size) {
  PlasmaObject result = {};

  // Immediately fulfill it using the fallback allocator.
  if (RayConfig::instance().plasma_unlimited()) {
    PlasmaError error = create_callback(/*fallback_allocator=*/true, &result,
                                        /*spilling_required=*/nullptr,
                                        /*callback=*/nullptr);
    return {result, error};
  }

  //if (!queue_.empty()) {
  //  // There are other requests queued. Return an out-of-memory error
  //  // immediately because this request cannot be served.
  //  return {result, PlasmaError::OutOfMemory};
  //}

  auto req_id = AddRequest(key, object_id, client, create_callback, object_size);
  if (!ProcessRequests().ok()) {
    // If the request was not immediately fulfillable, finish it.
    if (!queue_.empty()) {
      // Some errors such as a transient OOM error doesn't finish the request, so we
      // should finish it here.
      auto request_it = queue_.find(key);
      if (request_it != queue_.end()) {
        FinishRequest(request_it);
      }
    }
  }
  PlasmaError error;
  RAY_CHECK(GetRequestResult(req_id, &result, &error));
  return {result, error};
}

Status CreateRequestQueue::ProcessRequest(bool fallback_allocator,
                                          std::unique_ptr<CreateRequest> &request,
                                          bool *spilling_required,
                                          std::function<void(PlasmaError error)> callback) {
  request->error =
      request->create_callback(fallback_allocator, &request->result, spilling_required, callback);
  if (request->error == PlasmaError::OutOfMemory) {
    return Status::ObjectStoreFull("");
  } else if (request->error == PlasmaError::SpacePending) {
    return Status::TransientObjectStoreFull("");
  } else {
    return Status::OK();
  }
}

void CreateRequestQueue::HandleCreateReply(const ray::TaskKey &task_key, PlasmaError error) {
  auto request_it = queue_.find(task_key);
  if (request_it == queue_.end()) {
    return;
  }
  request_it->second->error = error;
  RAY_CHECK(error == PlasmaError::OutOfMemory ||
      error == PlasmaError::SpacePending ||
      error == PlasmaError::PreemptTask ||
      error == PlasmaError::FallbackAllocate) << "Unexpected error: " << flatbuf::EnumNamePlasmaError(error);

  auto now = get_time_();
  if (error == PlasmaError::SpacePending) {
    RAY_LOG(DEBUG) << "Waiting for space to become available before retrying " << task_key.second;
    return;
  }

  if (error == PlasmaError::PreemptTask) {
    // Return an error to the client. They should retry this task.
    FinishRequest(request_it);
    return;
  }

  if (error == PlasmaError::FallbackAllocate) {
    // Trigger the fallback allocator.
    auto status = ProcessRequest(/*fallback_allocator=*/true, request_it->second,
                                 /*spilling_required=*/nullptr, /*callback=*/nullptr);
    if (!status.ok()) {
      std::string dump = "";
      if (dump_debug_info_callback_) {
        dump = dump_debug_info_callback_();
      }
      RAY_LOG(INFO) << "Out-of-memory: Failed to create object "
                    << request_it->second->object_id << " of size "
                    << request_it->second->object_size / 1024 / 1024 << "MB\n"
                    << dump;
    }
    FinishRequest(request_it);
    return;
  }

  if (trigger_global_gc_) {
    trigger_global_gc_();
  }

  if (oom_start_time_ns_ == -1) {
    oom_start_time_ns_ = now;
  }
  auto grace_period_ns = oom_grace_period_ns_;
  auto spill_pending = spill_objects_callback_();
  if (spill_pending) {
    RAY_LOG(DEBUG) << "Reset grace period, spilling in progress";
    oom_start_time_ns_ = -1;
  } else if (now - oom_start_time_ns_ < grace_period_ns) {
    // We need a grace period since (1) global GC takes a bit of time to
    // kick in, and (2) there is a race between spilling finishing and space
    // actually freeing up in the object store.
    RAY_LOG(DEBUG) << "In grace period before fallback allocation / oom.";
  } else {
    if (plasma_unlimited_) {
      // Trigger the fallback allocator.
      auto status = ProcessRequest(/*fallback_allocator=*/true, request_it->second,
                                   /*spilling_required=*/nullptr, /*callback=*/nullptr);
      if (!status.ok()) {
        std::string dump = "";
        if (dump_debug_info_callback_) {
          dump = dump_debug_info_callback_();
        }
        RAY_LOG(INFO) << "Out-of-memory: Failed to create object "
                      << request_it->second->object_id << " of size "
                      << request_it->second->object_size / 1024 / 1024 << "MB\n"
                      << dump;
      }
    }
    FinishRequest(request_it);
  }
}

Status CreateRequestQueue::ProcessRequests() {
  // Suppress OOM dump to once per grace period.
  while (!queue_.empty() && !request_pending_) {
    auto request_it = queue_.begin();
    bool spilling_required = false;
    auto &task_key = request_it->first;
    auto status =
        ProcessRequest(/*fallback_allocator=*/false, request_it->second, &spilling_required,
            [this, task_key](PlasmaError error) {
              request_pending_ = false;
              HandleCreateReply(task_key, error);
            });
    if (spilling_required) {
      spill_objects_callback_();
    }
    if (status.ok() || status.IsObjectStoreFull()) {
      FinishRequest(request_it);
      // Reset the oom start time since the creation succeeds.
      oom_start_time_ns_ = -1;
    } else {
      request_pending_ = true;
    }
  }

  if (request_pending_) {
    return Status::TransientObjectStoreFull("");
  }
  return Status::OK();
}

void CreateRequestQueue::FinishRequest(
    absl::btree_map<ray::TaskKey, std::unique_ptr<CreateRequest>>::iterator request_it) {
  // Fulfill the request.
  auto req_id = request_it->second->request_id;
  RAY_CHECK(req_id != 0);
  auto it = fulfilled_requests_.find(req_id);
  RAY_CHECK(it != fulfilled_requests_.end());
  RAY_CHECK(it->second == nullptr);
  it->second = std::move(request_it->second);
  RAY_CHECK(num_bytes_pending_ >= it->second->object_size);
  num_bytes_pending_ -= it->second->object_size;
  index_.erase(req_id);
  queue_.erase(request_it);
}

void CreateRequestQueue::RemoveDisconnectedClientRequests(
    const std::shared_ptr<ClientInterface> &client) {
  for (auto it = queue_.begin(); it != queue_.end();) {
    if (it->second->client == client) {
      fulfilled_requests_.erase(it->second->request_id);
      RAY_CHECK(num_bytes_pending_ >= it->second->object_size);
      num_bytes_pending_ -= it->second->object_size;
      index_.erase(it->second->request_id);
      it = queue_.erase(it);
    } else {
      it++;
    }
  }

  for (auto it = fulfilled_requests_.begin(); it != fulfilled_requests_.end();) {
    if (it->second && it->second->client == client) {
      fulfilled_requests_.erase(it++);
    } else {
      it++;
    }
  }
}

}  // namespace plasma
