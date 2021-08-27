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


def test_spillback_scheduling(ray_start_cluster):
    # Submit many tasks that each require 1 CPU and produce a lot of memory.
    # Ray's default scheduling algorithm will cause these to be
    # disproportionately scheduled to the head node, causing spilling.
    # If we preempt and re-schedule tasks on remote nodes, we can avoid
    # spilling completely.
    config = {
        "lineage_pinning_enabled": True,
        "task_retry_delay_ms": 100,
        "worker_lease_timeout_milliseconds": 0,
        # This is needed to make sure that the scheduler respects
        # locality-based scheduling.
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=2, _system_config=config,
            object_store_memory=10 ** 8)
    for _ in range(4):
        cluster.add_node(num_cpus=2, object_store_memory=10 ** 8)

    ray.init(cluster.address)

    @ray.remote
    def f():
        return np.zeros(int(3.5 * 10**7), dtype=np.uint8)

    @ray.remote
    def done(x):
        return

    xs = [f.remote() for _ in range(10)]
    #for x in xs:
    #    ref = done.remote(x)
    #    print(ref, "depends on", x)
    #    ray.get(ref)
    ray.get([done.remote(x) for x in xs])

    summary = memory_summary(stats_only=True)
    print(summary)
    # Should expect 1 preempted task, no spilling.
    assert "Spill" not in summary
    assert "0 objects preempted" in summary


def test_spillback_scheduling_oom(ray_start_cluster):
    # Submit many tasks that each require 1 CPU and produce a lot of memory.
    # Unlike the above test, the total memory capacity will exceed the
    # cluster's capacity.
    # Check that the preemption based policy will still spill on each node.
    config = {
        "lineage_pinning_enabled": True,
        "task_retry_delay_ms": 100,
        "worker_lease_timeout_milliseconds": 0,
        # This is needed to make sure that the scheduler respects
        # locality-based scheduling.
        "scheduler_spread_threshold": 1.0,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=2, _system_config=config,
            object_store_memory=10 ** 8)
    for _ in range(4):
        cluster.add_node(num_cpus=2, object_store_memory=10 ** 8)

    ray.init(cluster.address)

    @ray.remote
    def f():
        return np.zeros(int(3.5 * 10**7), dtype=np.uint8)

    @ray.remote
    def done(x):
        return

    xs = [f.remote() for _ in range(20)]
    #for x in xs:
    #    ref = done.remote(x)
    #    print(ref, "depends on", x)
    #    ray.get(ref)
    ray.get([done.remote(x) for x in xs])

    summary = memory_summary(stats_only=True)
    print(summary)
    # Should expect 1 preempted task, no spilling.
    #assert "Spill" not in summary
    #assert "0 objects preempted" in summary


def test_shuffle(ray_start_cluster):
    # Submit many tasks that each require 1 CPU and produce a lot of memory.
    # Unlike the above test, the total memory capacity will exceed the
    # cluster's capacity.
    # Check that the preemption based policy will still spill on each node.
    config = {
        "lineage_pinning_enabled": True,
        "task_retry_delay_ms": 100,
        "worker_lease_timeout_milliseconds": 0,
        # This is needed to make sure that the scheduler respects
        # locality-based scheduling.
        "scheduler_spread_threshold": 1.0,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=4, _system_config=config,
            object_store_memory=10 ** 8)
    for _ in range(3):
        cluster.add_node(num_cpus=4, object_store_memory=10 ** 8)

    ray.init(cluster.address)

    dataset_size = 1_000_000_000

    @ray.remote
    def map(dataset_size, num_mappers, num_reducers):
        num_blocks = num_mappers * num_reducers
        partitions = [np.random.rand(int(dataset_size // 8 // num_blocks)) for _ in range(num_reducers)]
        return partitions

    @ray.remote
    def reduce(*args):
        return np.concatenate(args)

    @ray.remote
    def done(partition):
        return

    start = time.time()
    num_mappers = 16
    num_reducers = 16
    map_blocks = [map.options(num_returns=num_reducers).remote(dataset_size, num_mappers, num_reducers) for _ in range(num_mappers)]
    reduce_out = []
    for _ in range(num_reducers):
        args = [map_out.pop(0) for map_out in map_blocks]
        reduce_out.append(reduce.remote(*args))
        del args

    ray.get([done.remote(out) for out in reduce_out])
    end = time.time()
    print("Finished in", end - start)

    summary = memory_summary(stats_only=True)
    print(summary)
    # Should expect 1 preempted task, no spilling.
    #assert "Spill" not in summary
    #assert "0 objects preempted" in summary


def test_spillback_scheduling_pipeline(ray_start_cluster):
    # Submit many tasks that each require 1 CPU and produce a lot of memory.
    # Ray's default scheduling algorithm will cause these to be
    # disproportionately scheduled to the head node, causing spilling.
    # If we preempt and re-schedule tasks on remote nodes, we can avoid
    # spilling completely.
    config = {
        "lineage_pinning_enabled": True,
        "task_retry_delay_ms": 100,
        "worker_lease_timeout_milliseconds": 0,
        # This is needed to make sure that the scheduler respects
        # locality-based scheduling.
        "object_spilling_threshold": 2,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=2, _system_config=config,
            object_store_memory=10 ** 8)
    for _ in range(4):
        cluster.add_node(num_cpus=2, object_store_memory=10 ** 8)

    ray.init(cluster.address)

    @ray.remote
    def f():
        return np.zeros(int(3 * 10**7), dtype=np.uint8)

    @ray.remote
    def consumer(x):
        return x

    @ray.remote
    def done(x):
        return

    xs = [f.remote() for _ in range(10)]
    for i, x in enumerate(xs):
        print(i, x)
    xs = [consumer.remote(x) for x in xs]
    for i, x in enumerate(xs):
        print(i, x)
    #for x in xs:
    #    ref = done.remote(x)
    #    print(ref, "depends on", x)
    #    ray.get(ref)
    start = time.time()
    done = [done.remote(x) for x in xs]
    while time.time() - start < 30:
        _, done = ray.wait(done, timeout=1)
        if not done:
            break
    assert not done, f"{done} not ready"

    summary = memory_summary(stats_only=True)
    print(summary)
    # This test shouldn't spill anything but it can due to a bug where the task
    # gets spilled to a node that has more memory, but its arg is still in the
    # plasma store's creation queue.
    assert "Spill" not in summary, "This test is flaky"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
