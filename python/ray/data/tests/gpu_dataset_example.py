import ray.data
import time
import numpy as np

def produce_buffer(batch):
    time.sleep(1)
    return [np.random.rand(10) for _ in range(len(batch))]

def consume_buffer(batch):
    time.sleep(1)
    return [sum(x) for x in batch]

actor_pool_strategy = ray.data.ActorPoolStrategy(5, 5)
ds = (ray.data.range(10)
        .map_batches(produce_buffer, compute=actor_pool_strategy)
        .map_batches(consume_buffer, compute=actor_pool_strategy))
print(ds.take())
