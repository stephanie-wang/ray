import ray
import numpy as np

NUM_PARTITION = 4

@ray.remote
def map(data, npartitions):
    outputs = [list() for _ in range(NUM_PARTITION)]
    for row in data:
        outputs[int(row * NUM_PARTITION)].append(row)
    return tuple(sorted(output) for output in outputs)

@ray.remote
def reduce(*partitions):
    # Flatten and sort the partitions.
    return sorted(row for partition in partitions for row in partition)

ray.init()
dataset = [np.random.rand(12) for _ in range(NUM_PARTITION)]  # Random floats from the range [0, 1).
map_outputs = [
    map.options(num_returns=NUM_PARTITION).remote(partition, NUM_PARTITION)
    for partition in dataset]
outputs = []
for i in range(NUM_PARTITION):
    # Gather one output from each map task.
    outputs.append(reduce.remote(*[partition[i] for partition in map_outputs]))
print(ray.get(outputs))
