#include <list>

#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

class TaskLogger {
 public:
  TaskLogger() {}

  void LogRequest(const rpc::PushTaskRequest &request);

 private:
  std::list<const rpc::PushTaskRequest> log_;
};

}  // namespace ray
