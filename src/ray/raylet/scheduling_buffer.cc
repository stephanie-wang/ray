#include "ray/raylet/scheduling_buffer.h"

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

SchedulingBuffer::SchedulingBuffer(size_t max_decision_buffer, size_t max_push_buffer)
    : max_decision_buffer_(max_decision_buffer),
      max_push_buffer_(max_push_buffer) {}

ClientID SchedulingBuffer::GetDecision(const ObjectID &object_id) const {
  const TaskID task_id = ComputeTaskId(object_id);
  auto it = task_decision_buffer_.find(task_id);
  if (it != task_decision_buffer_.end()) {
    return it->second;
  } else {
    return ClientID::nil();
  }
}

void SchedulingBuffer::AddPush(const ObjectID &argument_id, const ClientID &client_id) {
  // If the argument is local, then push it to the receiving node.
  auto inserted = push_requests_.insert({argument_id, {client_id}});
  if (inserted.second) {
    push_request_its_.push_back(argument_id);
    if (push_requests_.size() > max_push_buffer_) {
      RAY_LOG(INFO) << "Evicting push request " << push_request_its_.front();
      push_requests_.erase(push_request_its_.front());
      push_request_its_.pop_front();
    }
  } else {
    inserted.first->second.push_back(client_id);
  }
}

void SchedulingBuffer::AddDecision(const Task &task, const ClientID &client_id) {
  const TaskID task_id = task.GetTaskSpecification().TaskId();
  RAY_LOG(INFO) << "Added decision " << task_id << " on client " << client_id;

  decision_buffer_[client_id].push_back(task_id);
  if (decision_buffer_[client_id].size() > max_decision_buffer_) {
    RAY_LOG(INFO) << "Evicting decision " << decision_buffer_[client_id].front() << " on client " << client_id;
    decision_buffer_[client_id].pop_front();
  }

  bool is_actor_task = task.GetTaskSpecification().IsActorTask();
  const ActorID actor_id = task.GetTaskSpecification().ActorId();

  for (int i = 0; i < task.GetTaskSpecification().NumArgs(); ++i) {
    int count = task.GetTaskSpecification().ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      ObjectID argument_id = task.GetTaskSpecification().ArgId(i, j);
      AddPush(argument_id, client_id);
      RAY_LOG(INFO) << "Added push request " << argument_id;

      if (is_actor_task) {
        actor_push_requests_[actor_id].push_back(argument_id);
        if (actor_push_requests_[actor_id].size() > max_push_buffer_) {
          actor_push_requests_[actor_id].pop_front();
        }
      }
    }
  }

  task_decision_buffer_[task_id] = client_id;
  task_decision_buffer_its_.push_back(task_id);
  if (task_decision_buffer_its_.size() > max_push_buffer_) {
    task_decision_buffer_.erase(task_decision_buffer_its_.front());
    task_decision_buffer_its_.pop_front();
  }
} 

void SchedulingBuffer::UpdateActorPushes(const ActorID &actor_id, const ClientID &client_id) {
  for (const auto &argument_id : actor_push_requests_[actor_id]) {
    //AddPush(argument_id, client_id);
    push_requests_[argument_id].push_back(client_id);
  }
}

std::vector<std::pair<ObjectID, ClientID>> SchedulingBuffer::GetPushes(const ClientID &client_id) {
  std::vector<std::pair<ObjectID, ClientID>> pushes;
  for (const auto task_id : decision_buffer_[client_id]) {
    // Hack - this won't work for multiple return values or puts...
    const ObjectID return_id = ComputeReturnId(task_id, 1);
    auto it = push_requests_.find(return_id);
    if (it != push_requests_.end()) {
      for (const auto &push_to_client_id : it->second) {
        if (push_to_client_id == client_id) {
          continue;
        }
        PushRequest push;
        push.push_from_client_id = client_id;
        push.object_id = return_id;
        push.push_to_client_id = push_to_client_id;
        auto inserted = previous_pushes_.insert(push);
        if (inserted.second) {
          pushes.push_back({return_id, push_to_client_id});

          previous_push_its_.push_back(push);
          if (previous_pushes_.size() > max_push_buffer_) {
            previous_pushes_.erase(previous_push_its_.front());
            previous_push_its_.pop_front();
          }
        }
      }
    }
  }

  return pushes;
}

}  // namespace raylet

}  // namespace ray
