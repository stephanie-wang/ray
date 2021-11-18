import ray
import csv
import argparse
import numpy as np
import time
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=4)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=250_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/pipeline.csv")
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=10)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']

def test_ray_shuffle():
    shuffle_start = perf_counter()

    @ray.remote
    def map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        return True

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    refs = []
    for i in range(WORKING_SET_RATIO):
        refs.append([map.options(num_returns=npartitions).remote(npartitions)
            for _ in range(npartitions)])
    results = []
    for i in range(WORKING_SET_RATIO):
        for j in range(npartitions):
            results.append(reduce.remote(*[ref[j] for ref in refs[i]]))
    ray.get(results)

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

def test_baseline_shuffle():
    shuffle_start = perf_counter()

    @ray.remote
    def map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        return True

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    for _ in range(WORKING_SET_RATIO):
        map_outputs = [
                map.options(num_returns=npartitions).remote(npartitions)
                for _ in range(npartitions)]
        outputs = []
        for i in range(npartitions):
            outputs.append(reduce.remote(*[partition[i] for partition in map_outputs]))
        ray.get(outputs)

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

ray.init(object_store_memory=OBJECT_STORE_SIZE)

ray_time = []
base_time = []
for i in range(NUM_TRIAL):
    ray_time.append(test_ray_shuffle())
    base_time.append(test_baseline_shuffle())

print(f"Baseline Shuffle time: {sum(base_time)/NUM_TRIAL}")
print(f"Ray Shuffle time: {sum(ray_time)/NUM_TRIAL}")

#header = ['base_var','ray_var','working_set_ratio', 'object_store_size','object_size','baseline_pipeline','ray_pipeline']
data = [np.var(base_time), np.var(ray_time), WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(base_time)/NUM_TRIAL, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)
