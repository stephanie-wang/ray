#include "ray/core_worker/transport/task_logger.h"

#include "ray/util/logging.h"
#include <iostream>
#include <fstream>

namespace ray {

void TaskLogger::LogRequest(const rpc::PushTaskRequest &request, const WorkerID worker_id) {
  RAY_LOG(DEBUG) << "Logging request";
  std::ofstream persistent;
  persistent.open(std::format("~/Documents/ray-src/worker_log_{}.txt", WorkerID::FromBinary(worker_id)))
  request.SerializeToStream(persistent);
  // BaseEncoding.base64().encode(request.toByteArray())
  persistent.close();
  log_.push_back(request);
}

}  // namespace ray
