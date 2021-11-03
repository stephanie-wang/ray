import ray
import time
import sys
import argparse
import csv
import numpy as np
from time import perf_counter
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=50_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/pipeline.csv")
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']


def test_ray_pipeline():
    ray_pipeline_begin = perf_counter()

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        #args = ray.get(obj_ref)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_SIZE // 8)
        
    num_fill_object_store = OBJECT_STORE_SIZE//OBJECT_SIZE 
    refs = [producer.remote()  for _ in range(WORKING_SET_RATIO*num_fill_object_store)]
    new_refs = []
    for r in refs:
        new_refs.append(consumer.remote(r))

    for ref in new_refs:
        assert ray.get(ref)
    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

def test_baseline_pipeline():
    baseline_start = perf_counter()

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        #args = ray.get(obj_ref)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_SIZE // 8)
        
    num_fill_object_store = OBJECT_STORE_SIZE//OBJECT_SIZE
    for i in range(WORKING_SET_RATIO):
        refs = []
        for j in range(num_fill_object_store):
            refs.append(producer.remote())

        new_refs = []
        for j in range(num_fill_object_store):
            new_refs.append(consumer.remote(refs[j]))

        assert ray.get(new_refs)


    baseline_end = perf_counter()
    return baseline_end - baseline_start

ray.init(object_store_memory=OBJECT_STORE_SIZE)

baseline_time = test_baseline_pipeline()
ray_pipeline_time = test_ray_pipeline()

#header = ['working_set_ratio', 'object_store_size','object_size','baseline_pipeline','ray_pipeline']
data = [WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, baseline_time, ray_pipeline_time]

with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)

print(f"Baseline Pipieline time: {baseline_time}")
print(f"Ray Pipieline time: {ray_pipeline_time}")
