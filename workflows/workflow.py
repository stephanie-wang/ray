import ray
from ray import workflow


@workflow.step
def load_data():
    pipes = ray.data.range(1000) \
            .map(lambda x: x * 2) \
            .repeat() \
            .random_shuffle() \
            .split(n=3)
    return [pipe.iter_datasets() for pipe in pipes]


@ray.remote
def consume(i, batch):
    num_rows = batch.count()
    print("consume", i, num_rows)
    return num_rows


@workflow.step
def train(pipes, total=0):
    # What level of fault tolerance do we need to checkpoint a pipe? How do app vs system level lineage interact?
    batches = [next(pipe) for pipe in pipes]
    results = [consume.remote(i, batch) for i, batch in enumerate(batches)]
    total += sum(ray.get(results))
    # Stopping condition.
    if total >= 3_000:
        return total
    else:
        return train.step(pipes, total)


if __name__ == '__main__':
    ray.init()

    workflow.init()
    data = load_data.step()
    output = train.step(data)
    print(output.run())
