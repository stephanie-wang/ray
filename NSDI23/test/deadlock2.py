##########################################################
#       Deadlock#2. Resource Cycle between producer and consumer.
#       aka. When effective working set is greater than the object store size
# Solution 1. Find the resource cycle via comparing resource demands

# Check the performance diff between deadlock#1 on simple shuffle to see if Deadlock#2 improved performance
##########################################################
import ray
import csv
import argparse
import numpy as np 
import time
import random
from time import perf_counter
from termcolor import colored

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=30)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
NUM_WORKER = params['NUM_WORKER']
OBJECT_STORE_BUFFER_SIZE = 5_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def pipeline():
    @ray.remote(num_cpus=1)
    def consumer(a, b, c):
        return True

    @ray.remote(num_cpus=1) 
    def small_obj_producer(): 
        obj_size = (OBJECT_STORE_SIZE)//64
        return np.zeros(obj_size)

    @ray.remote(num_cpus=1) 
    def large_obj_producer(): 
        obj_size = (OBJECT_STORE_SIZE + 100_000_000)//16
        return np.zeros(obj_size)

    small_obj = small_obj_producer.remote()
    time.sleep(3)
    print("Done Sleeping")

    large_obj1 = large_obj_producer.remote()
    large_obj2 = large_obj_producer.remote()
    res = consumer.remote(large_obj1, large_obj2, small_obj)


    return ray.get(res)

def all_workers_spinning():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def fill_object_store(): 
        obj_size = (OBJECT_STORE_SIZE)//8
        return np.zeros(obj_size)

    @ray.remote(num_cpus=1) 
    def producer(): 
        obj_size = (OBJECT_STORE_SIZE//NUM_WORKER)//8
        print(obj_size)
        return np.zeros(obj_size)

    #Fill the object store
    filling_obj = fill_object_store.remote()
    time.sleep(3)

    # Submit producers so all workers are spinning
    objs = []
    for _ in range(NUM_WORKER):
        objs.append(producer.remote())

    res = consumer.remote(objs)
    print("Calling filling_obj consumer")
    r = consumer.remote(filling_obj)
    ray.get(res)
    ray.get(r)
    print("Called filling_obj consumer")

    del objs

    return True

def gather():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        obj_size = (OBJECT_STORE_SIZE//NUM_WORKER)//8
        obj_size += (OBJECT_STORE_BUFFER_SIZE // 4)
        return np.zeros(obj_size)

    #Produces over object_store size 
    objs = []
    for _ in range(NUM_WORKER):
        objs.append(producer.remote())

    res = consumer.remote(objs)
    ray.get(res)

    return True

def scatter_gather():
    @ray.remote
    def scatter(npartitions, object_store_size):
        size = (object_store_size // 8)//npartitions
        #return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))
        return tuple(np.random.randint(1<<31, size=size) for i in range(npartitions))

    @ray.remote
    def worker(partitions):
        time.sleep(1)
        return np.average(partitions)

    @ray.remote
    def gather(*avgs):
        return np.average(avgs)


    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    scatter_outputs = [scatter.options(num_returns=npartitions).remote(npartitions, OBJECT_STORE_SIZE) for _ in range(WORKING_SET_RATIO)]
    outputs = [[] for _ in range(WORKING_SET_RATIO)]
    for i in range(WORKING_SET_RATIO):
        for j in range(npartitions):
            outputs[i].append(worker.remote(scatter_outputs[i][j]))
    del scatter_outputs
    gather_outputs = []
    for i in range(WORKING_SET_RATIO):
        gather_outputs.append(gather.remote(*[o for o in outputs[i]]))
    del outputs
    ray.get(gather_outputs)

    del gather_outputs
    return True

def shuffle():
    @ray.remote
    def map(object_size):
        size = object_size//NUM_WORKER
        data = np.zeros(object_size)
        return tuple(data[(i*size):((i+1)*size)] for i in range(NUM_WORKER))

    @ray.remote
    def reduce(*partitions):
        return True


    #Set object size to overcommit the object store
    object_size = OBJECT_STORE_SIZE//(NUM_WORKER*8)
    object_size += (OBJECT_STORE_BUFFER_SIZE //8)

    #Map
    refs = [map.options(num_returns=NUM_WORKER).remote(object_size)
            for _ in range(NUM_WORKER)]

    results = []
    for j in range(NUM_WORKER):
        results.append(reduce.remote(*[ref[j] for ref in refs]))
    print("reducer submitted")
    del refs
    ray.get(results)
    del results

    return True

ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)

'''
print('\n**** Starting all_workers_spinning() ****')
if all_workers_spinning():
    print("All workers spinning detected and resolved")
print('\n**** Starting pipeline() ****')
pipeline()
print('\n\t  Pipeline Finished')
'''

#Resouce Cycle
print('\n**** Starting shuffle() ****')
if (shuffle()):
    print("Resource cycle detected and resolved")
'''
print('\n**** Starting gather() ****')
if (gather()):
    print("Resource cycle detected and resolved")
if (scatter_gather()):
    print("Resource cycle detected and resolved")
print('\n**** Starting shuffle() ****')
if (shuffle()):
    print("Resource cycle detected and resolved")
print('\n**** Starting parallel-shuffle() ****')
if (shuffle()):
    print("Resource cycle detected and resolved")
'''
