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


def test_simple(ray_start_cluster):
    config = {
        "lineage_pinning_enabled": True,
        "max_direct_call_object_size": 10,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=1, _system_config=config)

    ray.init(cluster.address)

    @ray.remote
    def f():
        time.sleep(1)

    xs = [f.remote() for _ in range(3)]
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
    assert (end - start) >= 1

def test_deps(ray_start_cluster):
    config = {
        "lineage_pinning_enabled": True,
        "max_direct_call_object_size": 10,
        "worker_lease_timeout_milliseconds": 0,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=1, _system_config=config)

    ray.init(cluster.address)

    @ray.remote
    def f():
        time.sleep(1)

    @ray.remote
    def g(x):
        return

    x = f.remote()
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
    assert (end - start) >= 1

def test_deps_pending(ray_start_cluster):
    config = {
        "lineage_pinning_enabled": True,
        "max_direct_call_object_size": 10,
        "worker_lease_timeout_milliseconds": 0,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=1, _system_config=config)

    ray.init(cluster.address)

    @ray.remote
    def f():
        time.sleep(1)

    @ray.remote
    def sleep():
        while True:
            pass

    @ray.remote
    def g(x):
        return

    x = f.remote()
    ray.get(x)
    start = time.time()
    ray.get(x, timeout=0)
    end = time.time()
    print("first get done", end - start)

    s = sleep.remote()
    time.sleep(1)  # Let sleep task be scheduled.
    y = g.remote(x)  # Only 1 CPU, so g should now be queued.
    ready, _ = ray.wait([y], timeout=1)
    assert not ready

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
    assert (end - start) >= 1

if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
