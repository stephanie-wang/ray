import ray
import time
import sys
import argparse
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--LIST_SIZE', '-l', type=int, default=1000)
args = parser.parse_args()
params = vars(args)

LIST_SIZE = params['LIST_SIZE']
OBJECT_STORE_SIZE = 1000_000_000 # 1GB


def test_ray_pipeline():
    ray_pipeline_begin = perf_counter()
    @ray.remote(num_cpus=1)
    def consumer(args):
        #args = ray.get(obj_ref)
        time.sleep(5)
        return sum(sum(arg) for arg in args)

    @ray.remote(num_cpus=1)
    def producer():
        args = [[1 for _ in range(LIST_SIZE)] for _ in range(LIST_SIZE)]
        obj_ref = ray.put(args)
        result = consumer.remote(obj_ref)
        return ray.get(result) == LIST_SIZE * LIST_SIZE
        
    num_cpus = int(ray.cluster_resources()["CPU"])
    refs = []
    #refs = for [producer.remote() _ in range(num_cpus)]
    for _ in range(num_cpus):
        refs.append(producer.remote())

    for ref in refs:
        assert ray.get(ref)
    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

def test_baseline_pipeline():
    baseline_start = perf_counter()
    def consumer(*args):
        time.sleep(5)
        return sum(sum(arg) for arg in args)

    @ray.remote(num_cpus=1)
    def producer():
        args = [[1 for _ in range(LIST_SIZE)] for _ in range(LIST_SIZE)]
        result = consumer(*args)
        return result == LIST_SIZE * LIST_SIZE

    num_cpus = int(ray.cluster_resources()["CPU"])
    refs = []

    for _ in range(num_cpus):
        refs.append(producer.remote())

    for ref in refs:
        assert ray.get(ref)

    baseline_end = perf_counter()
    return baseline_end - baseline_start

ray.init(object_store_memory=OBJECT_STORE_SIZE)

baseline_time = test_baseline_pipeline()
ray_pipeline_time = test_ray_pipeline()

args = [[1 for _ in range(LIST_SIZE)] for _ in range(LIST_SIZE)]

print(f"Baseline Pipieline time: {baseline_time}")
print(f"Ray Pipieline time: {ray_pipeline_time} Memory Used:{sys.getsizeof(args)*int(ray.cluster_resources()['CPU'])*LIST_SIZE}")
