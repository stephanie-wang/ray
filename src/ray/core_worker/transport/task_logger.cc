#include "ray/core_worker/transport/task_logger.h"

#include "ray/util/logging.h"
#include <iostream>
#include <fstream>

namespace ray {

void TaskLogger::LogRequest(const rpc::PushTaskRequest &request, const ActorID actor_id) {
  RAY_LOG(DEBUG) << "Logging request";
  std::ofstream persistent;
  persistent.open("/Users/samyu/Documents/ray-src/actor_log_" + actor_id.Hex() + ".txt",
  	std::ofstream::out | std::ofstream::app | std::ios::binary);
  RAY_CHECK(persistent.good());
  request.SerializeToOstream(&persistent);
  RAY_CHECK(persistent.good());
  persistent.close();
  log_.push_back(request);
}

void TaskLogger::LogDuration(const int duration, const ActorID actor_id) {
  RAY_LOG(DEBUG) << "Logging duration";
  std::ofstream persistent;
  persistent.open("/Users/samyu/Documents/ray-src/actor_durations_" + actor_id.Hex() + ".txt",
	std::ofstream::out | std::ofstream::app);
  RAY_CHECK(persistent.good());
  persistent << duration;
  persistent << "\n";
  RAY_CHECK(persistent.good());
  persistent.close();
}


}  // namespace ray
