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
    SignalActor
)
from ray.internal.internal_api import memory_summary

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


def test_automatic_preempt_local_object(ray_start_cluster):
    # A <- B
    # C
    # C finishes first and consumes memory.
    # C gets preempted by A because A needs memory and has higher priority.  C
    # does not get scheduled again until after B finishes and releases A's
    # memory, since C's resource requirement now includes object store memory.
    config = {
        "lineage_pinning_enabled": True,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=2, _system_config=config,
            object_store_memory=10 ** 8)

    ray.init(cluster.address)

    s = SignalActor.remote()

    @ray.remote
    def f(c, s=None):
        c.inc.remote()
        if s is not None:
            ray.get(s.wait.remote())
        return np.zeros(8 * 10**7, dtype=np.uint8)

    @ray.remote
    def consume(x):
        return

    c = Counter.remote()
    # Submit  A <- B.
    long_task = consume.remote(f.remote(c, s))
    # Submit C.
    short_task = f.remote(c)

    # C finishes first.
    ray.get(short_task)
    # C get preempted by A.
    s.send.remote(clear=True)
    ray.get(long_task)
    # C gets reconstructed after preemption.
    ray.get(short_task)

    summary = memory_summary(stats_only=True)
    print(summary)
    assert "Spill" not in summary
    assert "1 objects preempted" in summary
    assert "0 tasks preempted" in summary


def test_automatic_preempt_running_task(ray_start_cluster):
    # A <- B
    # C
    # 1 CPU.
    # A finishes first and consumes memory.
    # C starts, acquires CPU.
    # B is submitted with higher priority than C.
    # When C finishes and tries to acquire memory held by A, we preempt C in
    # favor of running B.
    config = {
        "lineage_pinning_enabled": True,
        "task_retry_delay_ms": 100,
        "worker_lease_timeout_milliseconds": 0,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=1, _system_config=config,
            object_store_memory=10 ** 8)

    ray.init(cluster.address)

    s = SignalActor.remote()

    @ray.remote
    def f(c, s=None):
        c.inc.remote()
        if s is not None:
            # Let the driver know that we've been scheduled.
            s.send.remote()
            time.sleep(1)
        return np.zeros(8 * 10**7, dtype=np.uint8)

    @ray.remote
    def consume(x):
        return

    counter = Counter.remote()
    # Submit A and wait for it to complete.
    a = f.remote(counter)
    print("A", a)
    ray.get(a)

    # Submit C and wait for it to start running.
    c = f.remote(counter, s)
    print("C", c)
    ray.get(s.wait.remote())

    # Submit B.
    b = consume.remote(a)
    print("B", b)
    del a

    ray.get(b)
    ray.get(c)

    summary = memory_summary(stats_only=True)
    print(summary)
    # Should expect 1 preempted task, no spilling.
    assert "Spill" not in summary
    assert "0 objects preempted" in summary
    assert "1 tasks preempted" in summary


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
