#include "ray/raylet/scheduling_buffer.h"

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

SchedulingBuffer::SchedulingBuffer(size_t max_decision_buffer, size_t max_push_buffer, size_t max_actor_pushes)
    : max_decision_buffer_(max_decision_buffer),
      max_push_buffer_(max_push_buffer),
      max_actor_pushes_(max_actor_pushes) {}

ClientID SchedulingBuffer::GetDecision(const ObjectID &object_id) const {
  const TaskID task_id = ComputeTaskId(object_id);
  auto it = task_decision_buffer_.find(task_id);
  if (it != task_decision_buffer_.end()) {
    return it->second;
  } else {
    return ClientID::nil();
  }
}

void SchedulingBuffer::AddPush(const ObjectID &argument_id, const ClientID &client_id, const ActorID &actor_id) {
  // If the argument is local, then push it to the receiving node.
  auto inserted = push_requests_.insert({argument_id, {{client_id, actor_id}}});
  if (inserted.second) {
    push_request_its_.push_back(argument_id);
    if (push_requests_.size() > max_push_buffer_) {
      //RAY_LOG(INFO) << "Evicting push request " << push_request_its_.front();
      push_requests_.erase(push_request_its_.front());
      push_request_its_.pop_front();
    }
  } else {
    inserted.first->second.push_back({client_id, actor_id});
  }
}

void SchedulingBuffer::RecordActorPush(const ActorID &actor_id, const ObjectID &object_id) {
  actor_push_requests_[actor_id].push_back(object_id);
  if (actor_push_requests_[actor_id].size() > max_actor_pushes_) {
    actor_push_requests_[actor_id].pop_front();
  }
}

void SchedulingBuffer::AddDecision(const Task &task, const ClientID &client_id) {
  const TaskID task_id = task.GetTaskSpecification().TaskId();
  //RAY_LOG(INFO) << "Added decision " << task_id << " on client " << client_id;

  decision_buffer_[client_id].push_back(task_id);
  if (decision_buffer_[client_id].size() > max_decision_buffer_) {
    //RAY_LOG(INFO) << "Evicting decision " << decision_buffer_[client_id].front() << " on client " << client_id;
    decision_buffer_[client_id].pop_front();
  }

  const ActorID actor_id = task.GetTaskSpecification().ActorId();

  for (int i = 0; i < task.GetTaskSpecification().NumArgs(); ++i) {
    int count = task.GetTaskSpecification().ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      ObjectID argument_id = task.GetTaskSpecification().ArgId(i, j);
      AddPush(argument_id, client_id, actor_id);
      //RAY_LOG(INFO) << "Added push request " << argument_id;
    }
  }

  task_decision_buffer_[task_id] = client_id;
  task_decision_buffer_its_.push_back(task_id);
  if (task_decision_buffer_its_.size() > max_push_buffer_) {
    task_decision_buffer_.erase(task_decision_buffer_its_.front());
    task_decision_buffer_its_.pop_front();
  }
} 

std::vector<ObjectID> SchedulingBuffer::GetActorPushes(const ActorID &actor_id, const ClientID &local_node_id, const ClientID &push_to_client_id) {
  std::vector<ObjectID> pushes;
  for (const auto object_id : actor_push_requests_[actor_id]) {
    auto push = RecordPush(local_node_id, object_id, push_to_client_id);
    if (push) {
      pushes.push_back(object_id);
    }
  }
  return pushes;
}

bool SchedulingBuffer::RecordPush(const ObjectID object_id, const ClientID from_client_id, const ClientID &to_client_id) {
  PushRequest push;
  push.push_from_client_id = from_client_id;
  push.object_id = object_id;
  push.push_to_client_id = to_client_id;
  auto inserted = previous_pushes_.insert(push);
  if (inserted.second) {
    previous_push_its_.push_back(push);
    if (previous_pushes_.size() > max_push_buffer_) {
      previous_pushes_.erase(previous_push_its_.front());
      previous_push_its_.pop_front();
    }
    return true;
  } else {
    return false;
  }
}

std::pair<std::vector<std::pair<ObjectID, ClientID>>,
          std::unordered_set<ActorID>> SchedulingBuffer::GetPushes(const ClientID &client_id) {
  std::vector<std::pair<ObjectID, ClientID>> pushes;
  std::unordered_set<ActorID> actor_ids;
  for (const auto task_id : decision_buffer_[client_id]) {
    // Hack - this won't work for multiple return values or puts...
    const ObjectID return_id = ComputeReturnId(task_id, 1);
    auto it = push_requests_.find(return_id);
    if (it != push_requests_.end()) {
      for (const auto &push_request : it->second) {
        if (push_request.first == client_id) {
          continue;
        }
        auto push = RecordPush(return_id, client_id, push_request.first);
        if (push) {
          pushes.push_back({return_id, push_request.first});
          actor_ids.insert(push_request.second);
        }
      }
    }
  }

  return {pushes, actor_ids};
}

void SchedulingBuffer::RecordActorCreation(const ClientID &client_id) {
  actor_locations_.insert(client_id);
}

std::unordered_set<ClientID> SchedulingBuffer::GetActorLocations() const {
  return actor_locations_;
}


}  // namespace raylet

}  // namespace ray
