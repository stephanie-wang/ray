#include "task.h"

namespace ray {

namespace raylet {

flatbuffers::Offset<protocol::Task> Task::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  auto task = CreateTask(fbb, task_spec_.ToFlatbuffer(fbb),
                         task_execution_spec_.ToFlatbuffer(fbb));
  return task;
}

const TaskExecutionSpecification &Task::GetTaskExecutionSpec() const {
  return task_execution_spec_;
}

const TaskSpecification &Task::GetTaskSpecification() const { return task_spec_; }

void Task::SetExecutionDependencies(const std::vector<ObjectID> &dependencies) {
  task_execution_spec_.SetExecutionDependencies(dependencies);
  ComputeDependencies();
}

void Task::SetNumTasksExecuted(int64_t num_tasks_executed) {
  task_execution_spec_.SetNumTasksExecuted(num_tasks_executed);
}

void Task::IncrementNumForwards() { task_execution_spec_.IncrementNumForwards(); }

void Task::IncrementNumExecutions() {
  task_execution_spec_.IncrementNumExecutions();
}

void Task::IncrementNumResubmissions() {
  task_execution_spec_.IncrementNumResubmissions();
}

const std::vector<ObjectID> &Task::GetImmutableDependencies() const {
  return immutable_dependencies_;
}

const std::vector<ObjectID> &Task::GetDependencies() const { return dependencies_; }

void Task::ComputeImmutableDependencies() {
  immutable_dependencies_.clear();
  for (int i = 0; i < task_spec_.NumArgs(); ++i) {
    int count = task_spec_.ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      immutable_dependencies_.push_back(task_spec_.ArgId(i, j));
    }
  }
}

void Task::ComputeDependencies() {
  dependencies_.clear();
  dependencies_.assign(immutable_dependencies_.begin(), immutable_dependencies_.end());
  // TODO(atumanov): why not just return a const reference to ExecutionDependencies() and
  // avoid a copy.
  auto execution_dependencies = task_execution_spec_.ExecutionDependencies();
  dependencies_.insert(dependencies_.end(), execution_dependencies.begin(),
                       execution_dependencies.end());
}

void Task::CopyTaskExecutionSpec(const Task &task) {
  task_execution_spec_ = task.GetTaskExecutionSpec();
  ComputeDependencies();
}

void Task::AppendNondeterministicEvent(const std::string &nondeterministic_event) {
  task_execution_spec_.AppendNondeterministicEvent(nondeterministic_event);
}

std::string SerializeTaskAsString(const std::vector<ObjectID> *dependencies,
                                  const TaskSpecification *task_spec) {
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<ObjectID> execution_dependencies(*dependencies);
  TaskExecutionSpecification execution_spec(std::move(execution_dependencies));
  Task task(execution_spec, *task_spec);
  fbb.Finish(task.ToFlatbuffer(fbb));
  return std::string(fbb.GetBufferPointer(), fbb.GetBufferPointer() + fbb.GetSize());
}

}  // namespace raylet

}  // namespace ray
