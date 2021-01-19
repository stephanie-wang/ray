#include "ray/core_worker/transport/task_logger.h"

#include "ray/util/logging.h"
#include <iostream>
#include <fstream>

namespace ray {

void TaskLogger::LogRequest(const rpc::PushTaskRequest &request, const WorkerID worker_id) {
  RAY_LOG(DEBUG) << "Logging request";
  std::ofstream persistent;
  // persistent.open("/Users/accheng/Documents/ray_source/worker_log_" + worker_id.Hex() + ".txt",
  persistent.open("/home/ubuntu/ray_source/worker_log_" + worker_id.Hex() + ".txt",
  	std::ofstream::out | std::ofstream::app | std::ios::binary);
  // RAY_CHECK(persistent.good());
  request.SerializeToOstream(&persistent);
  // RAY_CHECK(persistent.good());
  persistent.close();
  log_.push_back(request);
}

}  // namespace ray
