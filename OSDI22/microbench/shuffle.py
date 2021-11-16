import ray
import argparse
import numpy as np

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--NUM_PARTITION', '-p', type=int, default = 4)
parser.add_argument('--NUM_LAYERS', '-l', type=int, default = 1)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=50_000_000)
args = parser.parse_args()
params = vars(args)

NUM_PARTITION = params['NUM_PARTITION']
NUM_LAYERS = params['NUM_LAYERS']
OBJECT_SIZE = params['OBJECT_SIZE'] 
OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 

def test_ray_shuffle():
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

ray.init(object_store_memory=OBJECT_STORE_SIZE)
dataset = [np.random.rand(8) for _ in range(NUM_PARTITION)]  # Random floats from the range [0, 1).
for partition in dataset:
    for row in partition:
        print(int(row*NUM_PARTITION), row) 
quit()
map_outputs = [
    map.options(num_returns=NUM_PARTITION).remote(partition, NUM_PARTITION)
    for partition in dataset]
outputs = []
for i in range(NUM_PARTITION):
    # Gather one output from each map task.
    outputs.append(reduce.remote(*[partition[i] for partition in map_outputs]))
print(ray.get(outputs))
