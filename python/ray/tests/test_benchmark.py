import os
import signal
import sys
import time
import csv

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


def test(num_nodes, num_consumers, object_size_mb, num_objects_per_node, backpressure):
    num_mb_spilled_initial = get_num_mb_spilled()

    @ray.remote
    def f():
        return np.zeros(int(object_size_mb * (10 ** 6)), dtype=np.uint8)

    @ray.remote
    def consume(x):
        return x

    @ray.remote
    def done(x):
        return

    start = time.time()
    num_objects = num_objects_per_node * num_nodes
    if backpressure:
        total_mem = ray.cluster_resources()['object_store_memory']
        # How many objects can fit in memory at once.
        max_tasks_in_flight = total_mem / (object_size_mb * num_objects_per_node)
        if num_consumers > 0:
            # If there are consumers, then we need space for both the task's
            # arg and its return value.
            max_tasks_in_flight /= 2
    else:
        # No backpressure. Submit all tasks at once.
        max_tasks_in_flight = num_objects

    num_tasks_submitted = 0
    while num_tasks_submitted < num_objects:
        num_tasks_to_submit = min(max_tasks_in_flight, num_objects - num_tasks_submitted)
        xs = [f.remote() for _ in range(num_tasks_to_submit)]
        num_tasks_submitted += num_tasks_to_submit

        for _ in range(num_consumers):
            xs = [consume.remote(x) for x in xs]
        done = [done.remote(x) for x in xs]

        # If we have consumers, delete the original refs so that we can GC.
        if (num_consumers > 0):
            print("Freeing refs", xs)
            del xs

        start_timeout = time.time()
        while time.time() - start_timeout < 360:
            _, done = ray.wait(done, timeout=1)
            if not done:
                break
        assert not done, f"{done} timed out"

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
    parser.add_argument("--num-consumers", type=int, default=0)
    parser.add_argument("--object-size-mb", type=int, required=True)
    parser.add_argument("--object-store-memory_mb", type=int, required=True)
    parser.add_argument("--num-objects-per-node", type=int, required=True)
    parser.add_argument('--output-filename', type=str, default=None)
    parser.add_argument('--local', action='store_true')

    args = parser.parse_args()
    backpressure = args.system == "application"
    if args.local:
        assert args.num_nodes == 1
        ray.init(object_store_memory=int(args.object_store_memory_mb * 10 ** 6))
    else:
        ray.init(address="auto")
    runtime, num_mb_spilled = test(args.num_nodes, args.num_consumers, args.object_size_mb, args.num_objects_per_node, backpressure)

    if args.output_filename is not None:
        fieldnames = [
                "system",
                "num_nodes",
                "num_consumers",
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
                "num_consumers": args.num_consumers,
                "object_size_mb": args.object_size_mb,
                "object_store_memory_mb": args.object_store_memory_mb,
                "num_objects_per_node": args.num_objects_per_node,
                "runtime": runtime,
                "spilled_mb": num_mb_spilled,
                })
