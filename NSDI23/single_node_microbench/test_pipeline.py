import ray
import csv
import argparse
import numpy as np 
import time
import multiprocessing
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=2)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=500_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=5)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def warmup():
    @ray.remote(num_cpus=1)
    def producer():
        return np.zeros(OBJECT_STORE_SIZE//(8*multiprocessing.cpu_count()))

    ref = []
    for i in range(multiprocessing.cpu_count()):
        ref.append(producer.remote())
    ray.get(ref)
    del ref


def test_ray_pipeline():
    ray_pipeline_begin = perf_counter()

    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return np.zeros(OBJECT_SIZE // 8)

    @ray.remote(num_cpus=1) 
    def producer(): 
        #time.sleep(0.1)
        return np.zeros(OBJECT_SIZE // 8)
        
    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES
    refs = [[] for _ in range(NUM_STAGES+1)]
    for _ in range(WORKING_SET_RATIO*num_fill_object_store):
        refs[0].append(producer.remote())

    for stage in range(1, NUM_STAGES):
        for i in range(WORKING_SET_RATIO*num_fill_object_store):
            refs[stage].append(consumer.remote(refs[stage-1][i]))
        del refs[stage-1]
            #del r

    for i in range(WORKING_SET_RATIO*num_fill_object_store):
        refs[NUM_STAGES].append(last_consumer.remote(refs[NUM_STAGES-1][i]))
        #del r

    del refs[NUM_STAGES -1]
    ray.get(refs[-1])
    del refs[-1]
    del refs

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

def test_baseline_pipeline():
    baseline_start = perf_counter()

    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return np.zeros(OBJECT_SIZE // 8)

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_SIZE // 8)
        
    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES
    for i in range(WORKING_SET_RATIO):
        refs = [[] for _ in range(NUM_STAGES+1)]
        for _ in range(num_fill_object_store):
            refs[0].append(producer.remote())

        for stage in range(1, NUM_STAGES):
            for r in refs[stage-1]:
                refs[stage].append(consumer.remote(r))
                #del r

        for r in refs[NUM_STAGES-1]:
            refs[NUM_STAGES].append(last_consumer.remote(r))
            #del r

        del refs[NUM_STAGES-1]
        ray.get(refs[-1])
        del refs[-1]
        del refs
    '''
    objs = []
    results = []
    Remain_tasks = num_fill_object_store*NUM_STAGES
    for _ in range(num_fill_object_store):
        objs.append(producer.remote())

    for o in objs:
        results.append(consumer.remote(o))
        del o

    while Remain_tasks > 0:
        ready, not_ready = ray.wait(results)
        num_returned_tasks = len(ready)
        ray.get(ready)
        Remain_tasks -= num_returned_tasks
        if Remain_tasks == 0:
            break
        results = not_ready
        num_returned_tasks = min(num_returned_tasks, Remain_tasks)
        for i in range(num_returned_tasks):
            objs.append(producer.remote())
        for i in range(num_returned_tasks):
            results.append(consumer.remote(objs[i]))
    '''

    baseline_end = perf_counter()
    return baseline_end - baseline_start

ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )

#Warm up tasks
warmup()

ray_time = []
base_time = []
for i in range(NUM_TRIAL):
    ray_time.append(test_ray_pipeline())
    base_time.append(test_baseline_pipeline())

#header = ['base_std','ray_std','base_var','ray_var','working_set_ratio', 'num_stages', 'object_store_size','object_size','baseline_pipeline','ray_pipeline']
data = [np.std(base_time), np.std(ray_time), np.var(base_time), np.var(ray_time), WORKING_SET_RATIO, NUM_STAGES, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(base_time)/NUM_TRIAL, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)

print(f"Baseline Pipieline time: {sum(base_time)/NUM_TRIAL}")
print(f"Ray Pipieline time: {sum(ray_time)/NUM_TRIAL}")
