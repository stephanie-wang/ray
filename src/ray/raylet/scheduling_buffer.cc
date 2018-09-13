#include "ray/raylet/scheduling_buffer.h"

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

SchedulingBuffer::SchedulingBuffer(size_t max_decision_buffer, size_t max_push_buffer)
    : max_decision_buffer_(max_decision_buffer),
      max_push_buffer_(max_push_buffer) {}

void SchedulingBuffer::AddDecision(const TaskID &task_id, const ClientID &client_id) {
  auto inserted = decision_buffer_.insert({task_id, client_id});
  RAY_CHECK(inserted.second) << task_id;
  decision_buffer_its_.push_back(inserted.first);
  if (decision_buffer_its_.size() > max_decision_buffer_) {
    auto evicted_decision = std::move(decision_buffer_its_.front());
    decision_buffer_.erase(evicted_decision);
    decision_buffer_its_.pop_front();
  }
} 

void SchedulingBuffer::ClearDecision(const TaskID &task_id) {
  auto it = decision_buffer_.find(task_id);
  if (it == decision_buffer_.end()) {
    return;
  }

  auto ptr = std::find(decision_buffer_its_.begin(), decision_buffer_its_.end(), it);
  RAY_CHECK(ptr != decision_buffer_its_.end());
  decision_buffer_its_.erase(ptr);
  decision_buffer_.erase(it);
} 

ClientID SchedulingBuffer::GetDecision(const ObjectID &object_id) const {
  TaskID task_id = ComputeTaskId(object_id);
  auto it = decision_buffer_.find(task_id);
  if (it == decision_buffer_.end()) {
    return ClientID::nil();
  } else {
    return it->second;
  }
}

bool SchedulingBuffer::AddPush(const ObjectID &object_id, const ClientID &client_id) {
  bool added = false;
  TaskID task_id = ComputeTaskId(object_id);
  auto it = decision_buffer_.find(task_id);
  if (it != decision_buffer_.end() && it->second != client_id) {
    push_buffer_[it->second].push_back({object_id, client_id});
    if (push_buffer_[it->second].size() > max_push_buffer_) {
      push_buffer_[it->second].pop_front();
    }
    added = true;
  }
  return added;
}

std::deque<std::pair<ObjectID, ClientID>> SchedulingBuffer::GetPushes(const ClientID &client_id) {
  auto pushes = std::move(push_buffer_[client_id]);
  push_buffer_[client_id].clear();
  return pushes;
}

}  // namespace raylet

}  // namespace ray
