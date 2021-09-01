import os
import signal
import sys
import time
import csv

import numpy as np
import pytest

import ray
from ray.cluster_utils import Cluster
from ray.test_utils import (
    wait_for_condition,
    wait_for_pid_to_exit,
    SignalActor
)
from ray.internal.internal_api import memory_summary


SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM

def get_num_mb_spilled():
    num_tries = 0
    summary = None
    while num_tries < 3:
        try:
            summary = memory_summary(stats_only=True)
            print(summary)
            break
        except:
            print("retrying collect stats")
        num_tries += 1

    if summary is None:
        num_mb_spilled = "x"
    if "Spilled" in summary:
        _, msg = summary.split("Spilled ")
        num_mb_spilled = int(msg.split(" ")[0])
    else:
        num_mb_spilled = 0
    return num_mb_spilled


def test(num_nodes, num_pipeline_args, object_size_mb, num_objects_per_node, backpressure):
    num_mb_spilled_initial = get_num_mb_spilled()

    @ray.remote
    def f():
        #time.sleep(object_size_mb * 0.01)
        return np.zeros(int(object_size_mb * (10 ** 6)), dtype=np.uint8)

    @ray.remote
    def consume(*x):
        #time.sleep(object_size_mb * 0.01)
        return np.concatenate(x)

    @ray.remote
    def done(x):
        return

    start = time.time()
    num_objects = num_objects_per_node * num_nodes
    if backpressure:
        total_mem = ray.cluster_resources()['object_store_memory']
        # How many objects can fit in memory at once.
        max_tasks_in_flight = (total_mem / 1e6) / (object_size_mb) * 0.8
        if num_pipeline_args > 0:
            # If there is a consumer, then we need space for both the consumer
            # task's args and its return value, the concatenated array.
            max_tasks_in_flight /= 2 * num_pipeline_args
        max_tasks_in_flight = int(max_tasks_in_flight)
    else:
        # No backpressure. Submit all tasks at once.
        max_tasks_in_flight = num_objects
    print("max tasks in flight", max_tasks_in_flight)

    num_tasks_submitted = 0
    while num_tasks_submitted < num_objects:
        num_tasks_to_submit = min(max_tasks_in_flight, num_objects - num_tasks_submitted)
        num_args = 1
        if num_pipeline_args > 0:
            num_args *= num_pipeline_args
        xs = [f.remote() for _ in range(num_tasks_to_submit * num_args)]
        num_tasks_submitted += num_tasks_to_submit

        if num_pipeline_args > 0:
            next_xs = []
            while xs:
                args = [xs.pop(0) for _ in range(num_pipeline_args)]
                next_xs.append(consume.remote(*args))
            xs = next_xs
        done_refs = [done.remote(x) for x in xs]

        # If we have consumers, delete the original refs so that we can GC.
        if num_pipeline_args > 0:
            print("Freeing refs", xs)
            del xs

        start_timeout = time.time()
        while time.time() - start_timeout < 360:
            _, done_refs = ray.wait(done_refs, timeout=1)
            if not done_refs:
                break
        assert not done_refs, f"{done_refs} timed out"

    runtime = time.time() - start
    print("Finished in", runtime)

    num_mb_spilled_final = get_num_mb_spilled()
    try:
        num_mb_spilled_final -= num_mb_spilled_initial
    except:
        num_mb_spilled_final = "x"
    print("Spilled", num_mb_spilled_final)
    return runtime, num_mb_spilled_final
    

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--system", type=str, required=True)
    parser.add_argument("--num-nodes", type=int, required=True)
    parser.add_argument("--object-size-mb", type=int, required=True)
    parser.add_argument("--object-store-memory-mb", type=int, required=True)
    parser.add_argument("--num-objects-per-node", type=int, required=True)
    parser.add_argument('--output-filename', type=str, default=None)
    # If this is 0, then we just produce objects and wait for them to be created.
    # If this is more than 0, then we submit a second round of consume tasks
    # that concatenates num_pipeline_args many objects into a single array.
    # Then, we release each consume task's output.
    parser.add_argument('--num-pipeline-args', type=int, default=0)
    parser.add_argument('--local', action='store_true')

    args = parser.parse_args()
    backpressure = args.system == "application"
    cluster = None
    if args.local:
        cluster = Cluster()
        num_cpus = 16
        object_store_memory = int(args.object_store_memory_mb * 10 ** 6)
        cluster.add_node(object_store_memory=object_store_memory,
                _system_config={
                    "lineage_pinning_enabled": True,
                    "task_retry_delay_ms": 0,
                    "worker_lease_timeout_milliseconds": 0,
                    "object_spilling_threshold": 2,
                    }, num_cpus=num_cpus)
        for _ in range(args.num_nodes - 1):
            cluster.add_node(object_store_memory=object_store_memory,
                    num_cpus=num_cpus)
        ray.init(address=cluster.address)
    else:
        ray.init(address="auto")
    runtime, num_mb_spilled = test(args.num_nodes, args.num_pipeline_args, args.object_size_mb, args.num_objects_per_node, backpressure)

    if args.output_filename is not None:
        fieldnames = [
                "system",
                "num_nodes",
                "num_pipeline_args",
                "object_size_mb",
                "object_store_memory_mb",
                "num_objects_per_node",
                "runtime",
                "spilled_mb",
                ]
        if not os.path.exists(args.output_filename) or os.stat(args.output_filename).st_size == 0:
            with open(args.output_filename, 'w') as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
        with open(args.output_filename, 'a') as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writerow({
                "system": args.system,
                "num_nodes": args.num_nodes,
                "num_pipeline_args": args.num_pipeline_args,
                "object_size_mb": args.object_size_mb,
                "object_store_memory_mb": args.object_store_memory_mb,
                "num_objects_per_node": args.num_objects_per_node,
                "runtime": runtime,
                "spilled_mb": num_mb_spilled,
                })
