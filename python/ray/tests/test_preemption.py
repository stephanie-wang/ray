import os
import signal
import sys
import time

import numpy as np
import pytest

import ray
from ray.test_utils import (
    wait_for_condition,
    wait_for_pid_to_exit,
)

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM

@ray.remote
class Counter:
    def __init__(self):
        self.num_executions = 0

    def inc(self):
        self.num_executions += 1

    def get(self):
        return self.num_executions

@ray.remote
def f(c):
    # TODO(memory): Hangs if we call ray.get here?
    c.inc.remote()


def test_simple(ray_start_cluster):
    config = {
        "lineage_pinning_enabled": True,
        "max_direct_call_object_size": 0,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=1, _system_config=config)

    ray.init(cluster.address)

    c = Counter.remote()
    xs = [f.remote(c) for _ in range(3)]
    ray.get(xs)
    start = time.time()
    ray.get(xs, timeout=0)
    end = time.time()
    print("first get done", end - start)

    worker = ray.worker.global_worker
    worker.core_worker.preempt(xs[0])
    start = time.time()
    print(ray.get(xs))
    end = time.time()
    print("DONE", end - start)

    # Make sure the task actually reran.
    assert ray.get(c.get.remote()) == 4

def test_deps(ray_start_cluster):
    config = {
        "lineage_pinning_enabled": True,
        "max_direct_call_object_size": 0,
        "worker_lease_timeout_milliseconds": 0,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=1, _system_config=config)

    ray.init(cluster.address)

    @ray.remote
    def g(x):
        return

    c = Counter.remote()
    x = f.remote(c)
    ray.get(x)
    start = time.time()
    ray.get(x, timeout=0)
    end = time.time()
    print("first get done", end - start)

    y = g.remote(x)
    worker = ray.worker.global_worker
    worker.core_worker.preempt(x)
    start = time.time()
    print(ray.get(y))
    end = time.time()
    print("DONE", end - start)

    # Make sure the task actually reran.
    assert ray.get(c.get.remote()) == 2

def test_deps_pending(ray_start_cluster):
    config = {
        "lineage_pinning_enabled": True,
        "max_direct_call_object_size": 0,
        "worker_lease_timeout_milliseconds": 0,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=1, _system_config=config)

    ray.init(cluster.address)

    @ray.remote
    def sleep():
        while True:
            pass

    @ray.remote
    def g(x):
        return

    c = Counter.remote()

    x = f.remote(c)
    ray.get(x)
    start = time.time()
    ray.get(x, timeout=0)
    end = time.time()
    print("first get done", end - start)

    s = sleep.remote()
    y = [None]
    def schedule_g():
        y[0] = g.remote(x)  # Only 1 CPU, so g should now be queued.
        ready, _ = ray.wait(y, timeout=1)
        return not ready
    wait_for_condition(schedule_g)

    worker = ray.worker.global_worker
    worker.core_worker.preempt(x)
    # Cancel s to give f and g the CPU.
    def cancel():
        print("try cancel")
        ray.cancel(s, force=True)
        try:
            ray.get(s, timeout=1)
        except ray.exceptions.GetTimeoutError:
            return False
        except ray.exceptions.WorkerCrashedError:
            return True
    wait_for_condition(cancel)

    start = time.time()
    print(ray.get(y))
    end = time.time()
    print("DONE", end - start)

    # Make sure the task actually reran.
    count = ray.get(c.get.remote())
    assert count == 2


def test_deps_pending_on_remote_node(ray_start_cluster):
    config = {
        "lineage_pinning_enabled": True,
        "max_direct_call_object_size": 0,
        "worker_lease_timeout_milliseconds": 0,
    }
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, _system_config=config, resources={"local": 1})
    cluster.add_node(num_cpus=1, resources={"remote": 1})

    ray.init(cluster.address)

    @ray.remote
    def sleep():
        while True:
            pass

    @ray.remote(resources={"remote": 1})
    def g(x):
        return

    c = Counter.remote()

    x = f.options(resources={"local": 1}).remote(c)
    ray.get(x)
    start = time.time()
    ray.get(x, timeout=0)
    end = time.time()
    print("first get done", end - start)

    s = [sleep.remote() for _ in range(2)]
    y = [None]
    def schedule_g():
        y[0] = g.remote(x)  # Only 1 CPU, so g should now be queued.
        ready, _ = ray.wait(y, timeout=1)
        return not ready
    wait_for_condition(schedule_g)

    worker = ray.worker.global_worker
    worker.core_worker.preempt(x)
    # Cancel s to give f and g the CPU.
    def cancel(s):
        print("try cancel")
        ray.cancel(s, force=True)
        try:
            ray.get(s, timeout=1)
        except ray.exceptions.GetTimeoutError:
            return False
        except ray.exceptions.WorkerCrashedError:
            return True
    wait_for_condition(lambda: cancel(s[0]))
    wait_for_condition(lambda: cancel(s[1]))

    start = time.time()
    print(ray.get(y, timeout=10))
    end = time.time()
    print("DONE", end - start)

    # Make sure the task actually reran.
    count = ray.get(c.get.remote())
    assert count == 2

if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
