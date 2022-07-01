import ray
import csv
import argparse
import numpy as np 
import time
import random
import multiprocessing
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=100_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=2)
parser.add_argument('--SEED', '-s', type=int, default=1)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_TRIAL = params['NUM_TRIAL']
SEED = params['SEED']
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


def test_ray_pipeline_dynamic():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        np.sort(obj_ref[:8388608])
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        #time.sleep(0.1)
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        
    ray_pipeline_begin = perf_counter()
    num_objs_to_produce = ((OBJECT_STORE_SIZE)//OBJECT_SIZE)
    objs = []
    res = []
    idx = 0
    for i in range(num_objs_to_produce):
        objs.append(producer.remote())
        objs.append(producer.remote())
        idx += 2
    time.sleep(0.2)
    for i in range(idx):
        res.append(consumer.remote(objs[i]))
    idx = 0
    for i in range(num_objs_to_produce):
        objs.append(producer.remote())
        objs.append(producer.remote())
        idx += 2
    time.sleep(0.2)
    for i in range(idx):
        res.append(consumer.remote(objs[i]))

    del objs
    ray.get(res)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

def test_ray_pipeline_random():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        np.sort(obj_ref[:8388608])
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        #time.sleep(0.1)
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        
    random.seed(SEED)
    num_objs_to_produce = ((WORKING_SET_RATIO*OBJECT_STORE_SIZE)//OBJECT_SIZE)
    num_objs_to_consume = num_objs_to_produce
    
    objs = []
    res = []
    objs_not_consumed = 0
    idx = 0

    ray_pipeline_begin = perf_counter()

    while num_objs_to_consume > 0:
        if num_objs_to_produce > 0:
            if random.randrange(2) > 0:
                n = random.randint(1, num_objs_to_produce)
                for i in range(n):
                    objs.append(producer.remote())
                objs_not_consumed += n
                num_objs_to_produce -= n
        else:
            n = random.randint(1, objs_not_consumed)
            if random.randrange(2):
                time.sleep(random.random())
            for i in range(n):
                res.append(consumer.remote(objs[idx]))
                idx += 1
            num_objs_to_consume -= n
            objs_not_consumed -= n



    del objs
    ray.get(res)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )

#Warm up tasks
warmup()

ray_time = []
for i in range(NUM_TRIAL):
    print('ray start')
    ray_time.append(test_ray_pipeline_dynamic())
    print('ray done')

'''
#header = ['base_std','ray_std','base_var','ray_var','working_set_ratio', 'num_stages', 'object_store_size','object_size','baseline_pipeline','ray_pipeline']
data = [np.std(base_time), np.std(ray_time), np.var(base_time), np.var(ray_time), WORKING_SET_RATIO, NUM_STAGES, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(base_time)/NUM_TRIAL, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)
'''

print(f"Ray Pipieline Dynamic time: {sum(ray_time)/NUM_TRIAL}")
