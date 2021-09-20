import ray
from ray import workflow


@workflow.step
def load_data(num_workers):
    pipes = ray.data.range(1000) \
            .map(lambda x: x * 2) \
            .repeat() \
            .random_shuffle() \
            .split(n=num_workers)
    return [pipe.iter_datasets() for pipe in pipes]

@ray.remote
class Worker:
    def __init__(self):
        # TODO: Reload model?
        pass

    def consume(self, batch):
        # ...Train...
        num_rows = batch.count()
        print("consume", i, num_rows)
        return num_rows


@workflow.step
def load_workers(num_workers):
    return [Worker.remote() for _ in range(num_workers)]


@workflow.step
def train(pipes, workers, total=0):
    batches = [next(pipe) for pipe in pipes]
    results = [worker.consume.remote(batch) for worker, batch in zip(workers, batches)]
    total += sum(ray.get(results))
    # Stopping condition.
    if total >= 3_000:
        return total
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
