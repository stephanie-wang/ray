import time
from collections import defaultdict
from collections import Counter
import numpy as np

from cython_examples import cython_process_batch, cython_process_batch2
from cython_examples import cython_process_batch3
from cython_examples import cython_process_batch4
from cython_examples import process_batch_reducer
from cython_examples import process_batch_reducer2
from cython_examples import process_batch_reducer3
from cython_examples import ReducerState
import yep


batch_size = 1000
sentence_length = 100

words = []
with open('words.txt', 'r') as f:
    for line in f.readlines():
        words.append(line.strip().encode('ascii'))
words = np.array(words)

num_reducers = 1
partition = {
        word: hash(word) % num_reducers for word in words
        }

batch = [np.string_.join(b' ', np.random.choice(words, sentence_length)) for _ in range(batch_size)]

with open('out', 'wb+') as f:
    for row in batch:
        f.write(row)
        f.write(b'\n')

def process_batch(batch, num_reducers):
    keyed_counts = {
            key: [] for key in range(num_reducers)
            }
    for row in batch:
        for word in row.split(b' '):
            keyed_counts[hash(word) % num_reducers].append((word, 1))

    return keyed_counts


start = time.time()
D = process_batch(batch, num_reducers)
end = time.time()
print("Map in python took", end - start)

start = time.time()
objects = []
# yep.start('/tmp/test.prof')
# for _ in range(50):
#     d = cython_process_batch(batch, num_reducers)
#     objects.append(d)
# yep.stop()
#d = cython_process_batch(batch, num_reducers)
#end = time.time()
#print("CYTHON: Took", end - start, type(d))

import gc
gc.disable()
start = time.time()
d = cython_process_batch3(batch, num_reducers)
end = time.time()
print("CYTHON (no gc): Took", end - start)

#start = time.time()
#a, b = cython_process_batch2(batch, num_reducers)
#end = time.time()
#print("CYTHON2: Took", end - start, type(b))



def reduce_batch(state, batch):
    for word in batch:
        if word not in state:
            state[word] = 0
        state[word] += 1

def reduce_counter(state, batch):
    state.update(batch)

state = {}
start = time.time()
reduce_batch(state, D[0])
end = time.time()
print("Reduce in python took:", end - start)

words = [word for word, _ in D[0]]
state = Counter()
start = time.time()
reduce_batch(state, words)
print(len(state))
print(len(words))
end = time.time()
print("Reduce COUNTER in python took:", end - start)

start = time.time()
state = {}
process_batch_reducer(state, words)
end = time.time()
print("Reduce in python took:", end - start)

start = time.time()
state = {}
process_batch_reducer2(state, words)
end = time.time()
print("Reduce 2 in python took:", end - start)


reducer = ReducerState()
start = time.time()
reducer.count(words)
#import yep
#yep.start('/tmp/pprof')
#for _ in range(100):
#    reducer.count(words)
#yep.stop()
end = time.time()
print("Reduce in python took:", end - start)


import ray
print("END-TO-END-TESTS")
ray.init()

start = time.time()
d = cython_process_batch3(batch, num_reducers)
x = ray.put(d)
d2 = ray.get(x)
process_batch_reducer2(state, d2[0])
end = time.time()
print("E2E test 1 took ", end - start)

start = time.time()
d = cython_process_batch4(batch, num_reducers)
x = ray.put(d)
d2 = ray.get(x)
process_batch_reducer3(state, d2[0])
end = time.time()
print("E2E test 2 took ", end - start)

reducer = ray._raylet.ReducerState()

start = time.time()
d = cython_process_batch4(batch, num_reducers)
x = ray.put(d)
d2 = ray.get(x)
reducer.count(d2[0])
end = time.time()
print("E2E test 3 took ", end - start)
