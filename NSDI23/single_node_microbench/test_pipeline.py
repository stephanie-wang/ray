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
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
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
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*multiprocessing.cpu_count())))

    ref = []
    for i in range(multiprocessing.cpu_count()):
        ref.append(producer.remote())
    ray.get(ref[-1])
    del ref


def test_ray_pipeline():
    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        np.sort(obj_ref[:8388608])
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return np.zeros(OBJECT_SIZE // 8)

    @ray.remote(num_cpus=1) 
    def producer(): 
        #time.sleep(0.1)
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        
    ray_pipeline_begin = perf_counter()

    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES
    refs = [[] for _ in range(NUM_STAGES+1)]
    for _ in range(WORKING_SET_RATIO*num_fill_object_store):
        refs[0].append(producer.remote())

    for stage in range(1, NUM_STAGES):
        for i in range(WORKING_SET_RATIO*num_fill_object_store):
            refs[stage].append(consumer.remote(refs[stage-1][i]))
        del refs[stage-1]

    for i in range(WORKING_SET_RATIO*num_fill_object_store):
        refs[NUM_STAGES].append(last_consumer.remote(refs[NUM_STAGES-1][i]))
        #del r

    del refs[NUM_STAGES -1]
    ray.get(refs[-1])

    ray_pipeline_end = perf_counter()

    del refs[-1]
    del refs

    return ray_pipeline_end - ray_pipeline_begin

def test_baseline_pipeline():
    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        np.sort(obj_ref[:8388608])
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return np.zeros(OBJECT_SIZE // 8)

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        
    baseline_start = perf_counter()

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

    baseline_end = perf_counter()
    return baseline_end - baseline_start

ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )

#Warm up tasks
warmup()

ray_time = []
base_time = []
for i in range(NUM_TRIAL):
    print('ray start')
    ray_time.append(test_ray_pipeline())
    print('ray done')
    base_time.append(test_baseline_pipeline())
    print('base done')

#header = ['base_std','ray_std','base_var','ray_var','working_set_ratio', 'num_stages', 'object_store_size','object_size','baseline_pipeline','ray_pipeline']
data = [np.std(base_time), np.std(ray_time), np.var(base_time), np.var(ray_time), WORKING_SET_RATIO, NUM_STAGES, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(base_time)/NUM_TRIAL, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)

print(f"Baseline Pipieline time: {sum(base_time)/NUM_TRIAL}")
print(f"Ray Pipieline time: {sum(ray_time)/NUM_TRIAL}")
