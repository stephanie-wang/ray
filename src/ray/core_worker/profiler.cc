#include "ray/core_worker/profiler.h"

namespace ray {

void Profiler::SetTaskSubmitTime(const TaskID &task_id, uint64_t time_us, const std::string &task_name) {
  absl::MutexLock lock(&mu_);
  RAY_CHECK(tasks_.count(task_id) == 0);
  tasks_[task_id].set_id(task_id.Hex());
  tasks_[task_id].set_submit_time_us(time_us);
  tasks_[task_id].set_name(task_name);
}

void Profiler::SetTaskDependencies(const TaskID &task_id,
                                   const std::vector<ObjectID> &deps) {
  absl::MutexLock lock(&mu_);
  auto &task = tasks_[task_id];
  for (const auto &dep : deps) {
    task.add_args(dep.Hex());
  }
}

void Profiler::SetTaskOutputs(const TaskID &task_id,
                              const std::vector<ObjectID> &outputs) {
  absl::MutexLock lock(&mu_);
  auto &task = tasks_[task_id];
  for (const auto &output : outputs) {
    task.add_outputs(output.Hex());
    objects_[output].set_id(output.Hex());
  }
}

void Profiler::SetTaskRuntime(const TaskID &task_id, uint64_t start_time_us,
                              uint64_t finish_time_us, uint64_t objects_stored_time_us) {
  absl::MutexLock lock(&mu_);
  auto &task = tasks_[task_id];
  task.set_start_time_us(start_time_us);
  task.set_finish_time_us(finish_time_us);
  task.set_objects_stored_time_us(objects_stored_time_us);
}

void Profiler::SetObjectSize(const ObjectID &object_id, uint64_t size) {
  absl::MutexLock lock(&mu_);
  objects_[object_id].set_size(size);
}

void Profiler::Dump(const std::string &tasks_filename,
                    const std::string &objects_filename) {
  absl::MutexLock lock(&mu_);

  rpc::Profile profile;
  for (const auto &task : tasks_) {
    auto t = profile.add_tasks();
    t->CopyFrom(task.second);
  }
  for (const auto &obj : objects_) {
    auto o = profile.add_objects();
    o->CopyFrom(obj.second);
  }

  std::ofstream out(tasks_filename, std::ofstream::out);
  profile.SerializeToOstream(&out);
  out.close();

  tasks_.clear();
  objects_.clear();
}

}  // namespace ray
