#ifndef RAY_RAYLET_SCHEDULING_BUFFER_H
#define RAY_RAYLET_SCHEDULING_BUFFER_H

#include <unordered_map>
#include <deque>

#include "ray/gcs/format/gcs_generated.h"
#include "ray/id.h"
#include "ray/raylet/task.h"

namespace ray {

namespace raylet {

class SchedulingBuffer {
 public:
  /// Create an actor registration.
  ///
  /// \param actor_table_data Information from the global actor table about
  /// this actor. This includes the actor's node manager location.
  SchedulingBuffer(size_t max_decision_buffer, size_t max_push_buffer, size_t max_actor_pushes);

  ClientID GetDecision(const ObjectID &object_id) const;

  void AddPush(const ObjectID &argument_id, const ClientID &client_id, const ActorID &actor_id);

  void RecordActorPush(const ActorID &actor_id, const ObjectID &object_id);

  std::vector<ObjectID> GetActorPushes(const ActorID &actor_id, const ClientID &local_node_id, const ClientID &push_to_client_id);

  void AddDecision(const Task &task, const ClientID &client_id);

  //void UpdateActorPushes(const ActorID &actor_id, const ClientID &client_id);
  std::pair<std::vector<std::pair<ObjectID, ClientID>>,
          std::unordered_set<ActorID>> GetPushes(const ClientID &client_id);

 private:
  struct PushRequest {
    ClientID push_from_client_id;
    ObjectID object_id;
    ClientID push_to_client_id;
    bool operator==(const PushRequest &rhs) const {
      return (push_from_client_id == rhs.push_from_client_id
          && object_id == rhs.object_id
          && push_to_client_id == rhs.push_to_client_id);
    }
  };

  struct push_hash {
    inline std::size_t operator()(const PushRequest & v) const {
      return v.push_from_client_id.hash() + v.object_id.hash() + v.push_to_client_id.hash();
    }
  };

  bool RecordPush(const ObjectID object_id, const ClientID from_client_id, const ClientID &to_client_id);

  std::unordered_map<TaskID, ClientID> task_decision_buffer_;
  std::deque<TaskID> task_decision_buffer_its_;
  std::unordered_map<ClientID, std::deque<TaskID>> decision_buffer_;
  std::unordered_map<ObjectID, std::vector<std::pair<ClientID, ActorID>>> push_requests_;
  std::deque<ObjectID> push_request_its_;
  std::unordered_set<PushRequest, push_hash> previous_pushes_;
  std::deque<PushRequest> previous_push_its_;
  std::unordered_map<ActorID, std::deque<ObjectID>> actor_push_requests_;

  size_t max_decision_buffer_;
  size_t max_push_buffer_;
  size_t max_actor_pushes_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_SCHEDULING_BUFFER_H
