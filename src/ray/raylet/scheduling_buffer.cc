#include "ray/raylet/scheduling_buffer.h"

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

SchedulingBuffer::SchedulingBuffer(size_t max_decision_buffer, size_t max_push_buffer)
    : max_decision_buffer_(max_decision_buffer),
      max_push_buffer_(max_push_buffer) {}

void SchedulingBuffer::AddDecision(const Task &task, const ClientID &client_id) {
  const TaskID task_id = task.GetTaskSpecification().TaskId();

  decision_buffer_[client_id].push_back(task_id);
  if (decision_buffer_[client_id].size() > max_decision_buffer_) {
    decision_buffer_[client_id].pop_front();
  }

  for (int i = 0; i < task.GetTaskSpecification().NumArgs(); ++i) {
    int count = task.GetTaskSpecification().ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      ObjectID argument_id = task.GetTaskSpecification().ArgId(i, j);
      // If the argument is local, then push it to the receiving node.
      auto inserted = push_requests_.insert({argument_id, {client_id}});
      if (inserted.second) {
        push_request_its_.push_back(argument_id);
        if (push_requests_.size() > max_push_buffer_) {
          push_requests_.erase(push_request_its_.front());
          push_request_its_.pop_front();
        }
      } else {
        inserted.first->second.push_back(client_id);
      }
    }
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
