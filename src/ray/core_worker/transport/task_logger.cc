#include "ray/core_worker/transport/task_logger.h"

#include "ray/util/logging.h"

namespace ray {

void TaskLogger::LogRequest(const rpc::PushTaskRequest &request) {
  RAY_LOG(DEBUG) << "Logging request";
  log_.push_back(request);
}

}  // namespace ray
