#include <fstream>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/core_worker.grpc.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

class Profiler {
 public:
  Profiler() {}

  void SetTaskSubmitTime(const TaskID &task_id, uint64_t time_us, const std::string &task_name);

  void SetTaskDependencies(const TaskID &task_id, const std::vector<ObjectID> &deps);

  void SetTaskOutputs(const TaskID &task_id, const std::vector<ObjectID> &returns);

  void SetTaskRuntime(const TaskID &task_id, uint64_t start_time_us, uint64_t end_time_us,
                      uint64_t objects_stored_time_us);

  void SetObjectSize(const ObjectID &object_id, uint64_t size);

  void Dump(const std::string &tasks_filename, const std::string &objects_filename);

 private:
  /// Mutex to protect the various maps below.
  mutable absl::Mutex mu_;

  std::unordered_map<TaskID, rpc::TaskProfile> tasks_;
  std::unordered_map<ObjectID, rpc::ObjectProfile> objects_;
};

}  // namespace ray
