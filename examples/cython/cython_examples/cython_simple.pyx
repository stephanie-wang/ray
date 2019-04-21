#!python
# cython: profile=True
# cython: linetrace=True
# cython: embedsignature=True, binding=True
# distutils: language = c++
# cython: language_level = 3
# distutils: define_macros=CYTHON_TRACE_NOGIL=1

from libcpp.unordered_map cimport unordered_map
from libcpp.pair cimport pair
from libcpp.string cimport string as c_string
from libcpp.unordered_set cimport unordered_set
from libcpp.map cimport map
from libcpp.list cimport list as c_list
from libcpp.vector cimport vector
from collections import defaultdict

from cpython.ref cimport PyObject
from cpython.dict cimport PyDict_GetItem, PyDict_SetItem

from cython.operator import dereference, postincrement

import time

def simple_blahfunc(x, y, z):
    return x + y + z


# Cython code directly callable from Python
def fib(n):
    if n < 2:
        return n
    return fib(n-2) + fib(n-1)


# Typed Cython code
def fib_int(int n):
    if n < 2:
        return n
    return fib_int(n-2) + fib_int(n-1)


# Cython-Python code
cpdef fib_cpdef(int n):
    if n < 2:
        return n
    return fib_cpdef(n-2) + fib_cpdef(n-1)


# C code
def fib_cdef(int n):
    return fib_in_c(n)


cdef int fib_in_c(int n):
    if n < 2:
        return n
    return fib_in_c(n-2) + fib_in_c(n-1)


# Simple class
class simple_class(object):
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

#def cython_process_batch(batch, num_reducers):
#    timestamp = batch[0][0]
#    timestamped = set(batch[0][1].split(b' '))
#
#    counts = defaultdict(int)
#    for _, row in batch:
#        for word in row.split(b' '):
#            counts[word] += 1
#
#    keyed_counts = {
#            key: [] for key in range(num_reducers)
#            }
#    for word, count in counts.items():
#        keyed_counts[hash(word) % num_reducers].append((timestamp if word in timestamped else 0, (word, count)))
#    return keyed_counts

def cython_process_batch(batch, int num_reducers):

    cdef:
        c_string row
        c_string word
        int h
        double timestamp

    keyed_counts = {key: [] for key in range(num_reducers)}
    for timestamp, row in batch:
        for word in row.split(b' '):
            h = hash(word)
            h = h % num_reducers
            keyed_counts[h].append((timestamp, word))

    return keyed_counts

def cython_process_batch2(batch, int num_reducers):

    cdef:
        c_string row
        c_string word
        int h
        double timestamp

    keyed_timestamps = [[] for key in range(num_reducers)]
    keyed_words = [[] for key in range(num_reducers)]
    for timestamp, row in batch:
        for word in row.split(b' '):
            h = hash(word)
            h = h % num_reducers
            keyed_timestamps[h].append(timestamp)
            keyed_words[h].append(word)

    return keyed_timestamps, keyed_words

def cython_process_batch3(batch, int num_reducers):

    cdef:
        int h

    keyed_words = [[] for key in range(num_reducers)]
    for row in batch:
        for word in row.split(b' '):
            h = hash(word)
            h = h % num_reducers
            keyed_words[h].append(word)

    return keyed_words

def process_batch_reducer(state, words):
    for word in words:
        if word not in state:
            state[word] = 0
        state[word] += 1

cpdef process_batch_reducer2(dict state, list words):
    cdef PyObject *obj
    cdef Py_ssize_t val
    for word in words:
        obj = PyDict_GetItem(state, word)
        if obj is NULL:
            state[word] = 1
        else:
            val = <object>obj
            state[word] = val + 1

cdef class ReducerState(object):
    cdef unordered_map[c_string, int] state

    def __init__(self):
        pass

    def count(self, words):
        cdef:
            c_string word

        for word in words:
            self.state[word] += 1
