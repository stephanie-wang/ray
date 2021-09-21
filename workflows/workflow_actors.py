import ray
from ray import workflow


@workflow.step
def load_data(num_workers):
    # Requirement 5: Recover data during execution without stopping the whole
    # job.
    pipes = ray.data.range(1000) \
            .map(lambda x: x * 2) \
            .repeat() \
            .random_shuffle() \
            .split(n=num_workers)
    return [pipe.iter_datasets() for pipe in pipes]

@ray.remote
class Worker:
    def __init__(self, model_checkpoint=None):
        if model_checkpoint is None:
            self.model = "xxx"
        else:
            self.model = load(model_checkpoint)

    def consume(self, batch):
        # ...Train...
        self.model, accuracy = update(model)
        return accuracy


@workflow.step
def load_workers(num_workers):
    return [Worker.remote() for _ in range(num_workers)]


@workflow.step
def train(pipes, workers, total=0):
    # Requirement 1: Checkpoint pipe without checkpointing the data. On
    # restart, re-execute the data pipeline.
    batches = [next(pipe) for pipe in pipes]
    # Requirement 2: Checkpoint actor state (the model). On restart, recover
    # actor from checkpoint.
    # Requirement 3: Periodic checkpointing of actor state to avoid checkpoint
    # model on each step.
    results = [worker.consume.remote(batch) for worker, batch in zip(workers, batches)]
    accuracy = sum(ray.get(results))
    # Stopping condition.
    if accuracy >= TARGET:
        # Requirement 4: Application dynamically chooses whether to checkpoint
        # a workflow step.
        return accuracy
    else:
        return train.step(pipes, total, workers)


if __name__ == '__main__':
    ray.init()

    workflow.init()
    num_workers = 3
    data = load_data.step(num_workers)
    workers = load_workers.step(num_workers)
    output = train.step(data, workers)
    print(output.run())
