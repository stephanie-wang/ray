#ifndef RAY_RAYLET_SCHEDULING_BUFFER_H
#define RAY_RAYLET_SCHEDULING_BUFFER_H

#include <unordered_map>
#include <deque>

#include "ray/gcs/format/gcs_generated.h"
#include "ray/id.h"

namespace ray {

namespace raylet {

class SchedulingBuffer {
 public:
  /// Create an actor registration.
  ///
  /// \param actor_table_data Information from the global actor table about
  /// this actor. This includes the actor's node manager location.
  SchedulingBuffer(size_t max_decision_buffer, size_t max_push_buffer);

  void AddDecision(const TaskID &task_id, const ClientID &client_id);

  bool AddPush(const ObjectID &object_id, const ClientID &client_id);

  std::deque<std::pair<ObjectID, ClientID>> GetPushes(const ClientID &client_id);

 private:
  std::deque<std::unordered_map<TaskID, ClientID>::iterator> decision_buffer_its_;
  std::unordered_map<TaskID, ClientID> decision_buffer_;
  std::unordered_map<ClientID, std::deque<std::pair<ObjectID, ClientID>>> push_buffer_;
  size_t max_decision_buffer_;
  size_t max_push_buffer_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_SCHEDULING_BUFFER_H
