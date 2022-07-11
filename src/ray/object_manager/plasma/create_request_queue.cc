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
#include <chrono>
#include <random>
#include <math.h>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/util/util.h"

namespace plasma {

uint64_t CreateRequestQueue::AddRequest(const ray::TaskKey &task_id,
                                        const ObjectID &object_id,
                                        const std::shared_ptr<ClientInterface> &client,
                                        const CreateObjectCallback &create_callback,
                                        size_t object_size) {
  auto req_id = next_req_id_++;
  fulfilled_requests_[req_id] = nullptr;
  // auto taskId = task.GetTaskSpecification().GetTaskKey();
  queue_.emplace(task_id, new CreateRequest(object_id, req_id, client, create_callback,
                                            object_size));
  num_bytes_pending_ += object_size;

  new_request_added_ = true;
  spinning_tasks_.RegisterTasks(object_id);
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
    const ObjectID &object_id, const std::shared_ptr<ClientInterface> &client,
    const CreateObjectCallback &create_callback, size_t object_size) {
  PlasmaObject result = {};

  // Immediately fulfill it using the fallback allocator.
  PlasmaError error =
      create_callback(/*fallback_allocator=*/true, &result,
                      /*spilling_required=*/nullptr, nullptr, nullptr, nullptr);
  return {result, error};
}

Status CreateRequestQueue::ProcessRequest(bool fallback_allocator,
                                          std::unique_ptr<CreateRequest> &request,
                                          bool *spilling_required,
                                          bool *block_tasks_required,
                                          bool *evict_tasks_required,
                                          ray::Priority *lowest_pri) {
  request->error =
      request->create_callback(fallback_allocator, &request->result, spilling_required,
                               block_tasks_required, evict_tasks_required, lowest_pri);
  if (request->error == PlasmaError::OutOfMemory) {
    return Status::ObjectStoreFull("");
  } else {
    return Status::OK();
  }
}

Status CreateRequestQueue::ProcessFirstRequest() {
  // Suppress OOM dump to once per grace period.
  bool logged_oom = false;
  auto queue_it = queue_.begin();
  if (queue_it != queue_.end()) {
    bool spilling_required = false;
    std::unique_ptr<CreateRequest> &request = queue_it->second;
    auto status = ProcessRequest(/*fallback_allocator=*/false, request,
                                 &spilling_required, nullptr, nullptr, nullptr);
    if (spilling_required) {
      spill_objects_callback_();
    }
    auto now = get_time_();
    if (status.ok()) {
      FinishRequest(queue_it);
      // Reset the oom start time since the creation succeeds.
      oom_start_time_ns_ = -1;
    } else {
      if (trigger_global_gc_) {
        trigger_global_gc_();
      }

      if (oom_start_time_ns_ == -1) {
        oom_start_time_ns_ = now;
      }
      auto grace_period_ns = oom_grace_period_ns_;
      auto spill_pending = spill_objects_callback_();
      if (spill_pending) {
        RAY_LOG(DEBUG) << "Reset grace period " << status << " " << spill_pending;
        oom_start_time_ns_ = -1;
        return Status::TransientObjectStoreFull("Waiting for objects to spill.");
      } else if (now - oom_start_time_ns_ < grace_period_ns) {
        // We need a grace period since (1) global GC takes a bit of time to
        // kick in, and (2) there is a race between spilling finishing and space
        // actually freeing up in the object store.
        RAY_LOG(DEBUG) << "In grace period before fallback allocation / oom.";
        return Status::ObjectStoreFull("Waiting for grace period.");
      } else {
        // Trigger the fallback allocator.
        status = ProcessRequest(/*fallback_allocator=*/true, request,
                                /*spilling_required=*/nullptr, nullptr, nullptr, nullptr);
        if (!status.ok()) {
          std::string dump = "";
          if (dump_debug_info_callback_ && !logged_oom) {
            dump = dump_debug_info_callback_();
            logged_oom = true;
          }
          RAY_LOG(INFO) << "Out-of-memory: Failed to create object "
                        << (request)->object_id << " of size "
                        << (request)->object_size / 1024 / 1024 << "MB\n"
                        << dump;
        }
        FinishRequest(queue_it);
      }
    }
  }
  return Status::OK();
}

void CreateRequestQueue::SetNewDependencyAdded() {
  RAY_LOG(DEBUG) << "[JAE_DEBUG] SetNewDependencyAdded Called ";
  new_dependency_added_ = true;
}

void CreateRequestQueue::SetShouldSpill(bool should_spill) {
  RAY_LOG(DEBUG) << "[JAE_DEBUG] SetShouldSpill Called "<<should_spill;
  should_spill_ = should_spill;
}

double CreateRequestQueue::GetSpillTime(){
  //Default 3 seconds
  //TODO(Jae) Set according to objects in the object store
  return RayConfig::instance().spill_wait_time();
}

static inline double distribution(double i, double b){
  return pow(((b-1)/b),(b-i))*(1/(b*(1-pow(1-(1/b),b))));
}

bool CreateRequestQueue::SkiRental(){
  static uint64_t ski_rental_timestamp;
  static bool ski_rental_started = false;
  uint64_t current_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>
							 (std::chrono::system_clock::now().time_since_epoch()).count();
  RAY_LOG(DEBUG) << "[JAE_DEBUG] SkiRental Called ";
  if(!ski_rental_started){
    ski_rental_started = true;
    ski_rental_timestamp = current_timestamp;
	return false;
  }
  if((current_timestamp - ski_rental_timestamp) >= GetSpillTime()){
	ski_rental_started = false;
  RAY_LOG(DEBUG) << "[JAE_DEBUG] SkiRental Return true ";
	return true;
  }
  return false;
  /*

  double diff = (double)(current_timestamp - ski_rental_timestamp);
  double avg = (double)((diff + last_called - ski_rental_timestamp)/2);
  double b = GetSpillTime();
  if(diff >= b)
    return true;
  double n = current_timestamp - last_called;
  bool spill = false;

  std::random_device rd;
  std::mt19937 gen(rd());
  double p = distribution(avg, b);
  std::discrete_distribution<> distrib({1-p, p});
  
  //SkiRental() is not called every timestep, thus compensate
  for(double i=0; i<n; i++){
    spill |= distrib(gen);
  }

  last_called = current_timestamp;
  if(spill)
    ski_rental_started = false;
  return spill;
  */
}

Status CreateRequestQueue::ProcessRequests() {
  // Suppress OOM dump to once per grace period.
  bool logged_oom = false;
  bool enable_blocktasks = RayConfig::instance().enable_BlockTasks();
  bool enable_blocktasks_spill = RayConfig::instance().enable_BlockTasksSpill();
  bool enable_evicttasks = RayConfig::instance().enable_EvictTasks();
  size_t num_spinning_workers = 0;

  //TODO(Jae) Check if taskId differs within a same task with multiple objects
  //Jae there are 2 choices for deadlock detection #1. 
  //1. When the first task is blocked register all tasks from queue_ to spinning_tasks
  //2. Iterate over other tasks in the queue and see if they fail too.
  while (!queue_.empty()) {
    auto queue_it = queue_.begin();
    bool spilling_required = false;
    bool block_tasks_required = false;
    bool evict_tasks_required = false;
    std::unique_ptr<CreateRequest> &request = queue_it->second;
    ray::Priority lowest_pri;
    auto status =
        ProcessRequest(/*fallback_allocator=*/false, request, &spilling_required,
                       &block_tasks_required, &evict_tasks_required, &lowest_pri);
    if (spilling_required && !enable_blocktasks_spill) {
      spill_objects_callback_();
    }

    auto now = get_time_();

    if (status.ok()) {
      //Turn these flags on only when matching flags are on
      block_tasks_required = (block_tasks_required&enable_blocktasks); 
	  evict_tasks_required = (evict_tasks_required&enable_evicttasks);

	  if(block_tasks_required || evict_tasks_required){
        RAY_LOG(DEBUG) << "[JAE_DEBUG] calling object_creation_blocked_callback (" 
			<< enable_blocktasks <<" " << enable_evicttasks << " " 
			<< enable_blocktasks_spill << ") on priority "
			<< lowest_pri;
	    on_object_creation_blocked_callback_(lowest_pri, block_tasks_required,
	  		  evict_tasks_required, false, num_spinning_workers, 0);
	  }

      FinishRequest(queue_it);
      // Reset the oom start time since the creation succeeds.
      oom_start_time_ns_ = -1;
    } else {
      if (trigger_global_gc_) {
        trigger_global_gc_();
      }
	  
	  if (oom_ == -1){
	    oom_ = now;
	  }

      if (oom_start_time_ns_ == -1) {
        oom_start_time_ns_ = now;
      }

	  if (enable_blocktasks || enable_evicttasks || enable_blocktasks_spill) {
        RAY_LOG(DEBUG) << "[JAE_DEBUG] calling object_creation_blocked_callback (" 
			<< enable_blocktasks <<" " << enable_evicttasks << " " 
			<< enable_blocktasks_spill << ") on priority "
			<< lowest_pri << " should_spill " << should_spill_;
		
          RAY_LOG(DEBUG) << "[JAE_DEBUG] Num of requests: "  << queue_.size();
		  should_spill_ |= SkiRental();
		if(new_dependency_added_ && new_request_added_ && enable_blocktasks_spill && !should_spill_){
          num_spinning_workers = spinning_tasks_.GetNumSpinningTasks();
	      on_object_creation_blocked_callback_(lowest_pri, enable_blocktasks, 
		      enable_evicttasks, true, num_spinning_workers, (request)->object_size);
		  //Check Deadlock only when a new object creation is requested
		  new_request_added_ = false;
  		  new_dependency_added_ = false;
		}else{
	      on_object_creation_blocked_callback_(lowest_pri, enable_blocktasks, 
		      enable_evicttasks, false, num_spinning_workers, (request)->object_size);
		}

	    if ((enable_blocktasks_spill || enable_evicttasks) && (!should_spill_)) { 
		  //should_spill is set in 2 cases
		  //1. block_spill callback gives #of spinning tasks to task_manager.
		  //Task_manager sets should_spill if #spinning_tasks==#leased_workers
		  //2. If evict_tasks does not evict any tasks
          return Status::TransientObjectStoreFull(
              "Waiting for higher priority tasks to finish");
		}
		//Alternative Design point. Always set should_spill_ from task_manager.
		//Current design reduce callback function. But spill called once only
		  RAY_LOG(DEBUG) << "[JAE_DEBUG] should_spill unset";
		//should_spill_ = false;
	  }

      auto grace_period_ns = oom_grace_period_ns_;
      auto spill_pending = spill_objects_callback_();
      if (spill_pending) {
        RAY_LOG(DEBUG) << "Reset grace period " << status << " " << spill_pending;
        oom_start_time_ns_ = -1;
        return Status::TransientObjectStoreFull("Waiting for objects to spill.");
      } else if (now - oom_start_time_ns_ < grace_period_ns) {
        // We need a grace period since (1) global GC takes a bit of time to
        // kick in, and (2) there is a race between spilling finishing and space
        // actually freeing up in the object store.
        RAY_LOG(DEBUG) << "In grace period before fallback allocation / oom.";
        return Status::ObjectStoreFull("Waiting for grace period.");
      } else {
        // Trigger the fallback allocator.
        RAY_LOG(DEBUG) << "fallback allocator called";
        auto status = ProcessRequest(/*fallback_allocator=*/true, request,
                                /*spilling_required=*/nullptr, nullptr, nullptr, nullptr);
        if (!status.ok()) {
          std::string dump = "";
          if (dump_debug_info_callback_ && !logged_oom) {
            dump = dump_debug_info_callback_();
            logged_oom = true;
          }
        RAY_LOG(INFO) << "Out-of-memory: Failed to create object "
          << (request)->object_id << " of size "
          << (request)->object_size / 1024 / 1024 << "MB\n"
          << dump;
        }
        FinishRequest(queue_it);
      }
    }
  }

  return Status::OK();
}

void CreateRequestQueue::FinishRequest(
    absl::btree_map<ray::TaskKey, std::unique_ptr<CreateRequest>>::iterator queue_it) {
  spinning_tasks_.UnRegisterSpinningTasks(queue_it->second->object_id);
  auto it = fulfilled_requests_.find(queue_it->second->request_id);
  RAY_CHECK(it != fulfilled_requests_.end());
  RAY_CHECK(it->second == nullptr);
  it->second = std::move(queue_it->second);
  RAY_CHECK(num_bytes_pending_ >= it->second->object_size);
  num_bytes_pending_ -= it->second->object_size;
  queue_it = queue_.erase(queue_it);
  //Reset this flag to test deadlock when an object is created (State Changed)
  //new_request_added_ = true;
    auto now = get_time_();
	  if(oom_!= -1){
	    RAY_LOG(DEBUG) << "[JAE_DEBUG] Spill Taken " << (now - oom_);
	  }
	  oom_ = -1;
}

void CreateRequestQueue::RemoveDisconnectedClientRequests(
    const std::shared_ptr<ClientInterface> &client) {
  for (auto it = queue_.begin(); it != queue_.end();) {
    if ((it->second)->client == client) {
      fulfilled_requests_.erase((it->second)->request_id);
      RAY_CHECK(num_bytes_pending_ >= (it->second)->object_size);
      num_bytes_pending_ -= (it->second)->object_size;
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
