import ray
from ray import workflow


def load_data():
    pipes = ray.data.range(1000) \
            .map(lambda x: x * 2) \
            .repeat() \
            .random_shuffle() \
            .split(n=3)
    return pipes


@ray.remote
def consume(i, batch):
    num_rows = batch.count()
    print("consume", i, num_rows)
    return num_rows


def train(pipes, total=0):
    pipe_iters = [pipe.iter_datasets() for pipe in pipes]
    batches = [next(pipe_iter) for pipe_iter in pipe_iters]
    results = [consume.remote(i, batch) for i, batch in enumerate(batches)]
    total += sum(ray.get(results))
    # Stopping condition.
    if total >= 3_000:
        return total
    else:
        return train(pipes, total)


if __name__ == '__main__':
    ray.init()

    data = load_data()
    output = train(data)
    print(output)
