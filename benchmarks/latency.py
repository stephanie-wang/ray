import cv2
import time
import json
import numpy as np
import threading
from collections import defaultdict
from ray import profiling
import os
import csv

import ray
import ray.cluster_utils

NUM_ITERATIONS = 1000
RESOURCE = "latency"

OWNERSHIP = "ownership"
BY_VALUE = "by_value"
LEASES = "leases"
CENTRALIZED = "centralized"


@ray.remote(num_cpus=0, resources={RESOURCE: 1})
def task(dep):
    return b"ok"

@ray.remote(num_cpus=0, resources={RESOURCE: 1})
def large_dep():
    return np.zeros(1 * 1024 * 1024, dtype=np.uint8)  # 1 MiB

@ray.remote(num_cpus=0, resources={RESOURCE: 1})
class Actor:
    def __init__(self):
        return

    def task(self, dep):
        return b"ok"


def run_with_actors_pipelined(num_rounds):
    dep = large_dep.remote()
    ray.get(dep)
    a = Actor.remote()
    ray.get(a.task.remote(None))

    start = time.time()

    deps = [a.task.remote(dep) for i in range(num_rounds)]
    ray.get(deps)

    duration = time.time() - start
    return [duration / num_rounds]

def run_with_actors_sync(num_rounds):
    dep = large_dep.remote()
    ray.get(dep)
    a = Actor.remote()
    ray.get(a.task.remote(None))

    start = time.time()

    latencies = []
    for i in range(num_rounds):
        start = time.time()
        ray.get(a.task.remote(dep))
        latencies.append(time.time() - start)
    return latencies


def run_pipelined(num_rounds):
    dep = large_dep.remote()
    ray.get(dep)

    start = time.time()

    deps = [task.remote(dep) for i in range(num_rounds)]
    ray.get(deps)

    duration = time.time() - start
    return [duration / num_rounds]

def run_sync(num_rounds):
    dep = large_dep.remote()
    ray.get(dep)

    start = time.time()

    latencies = []
    for i in range(num_rounds):
        start = time.time()
        ray.get(task.remote(dep))
        latencies.append(time.time() - start)
    return latencies

def main(args):
    configs = {
            OWNERSHIP: {
                "record_ref_creation_sites":0,
                "initial_reconstruction_timeout_milliseconds": 100,
                "num_heartbeats_timeout": 10,
                "lineage_pinning_enabled": 1,
                "free_objects_period_milliseconds": -1,
                "object_manager_repeated_push_delay_ms": 1000,
                "task_retry_delay_ms": 100,
                },
            LEASES: {
                "initial_reconstruction_timeout_milliseconds": 100,
                "num_heartbeats_timeout": 10,
                "object_manager_repeated_push_delay_ms": 1000,
                },
            CENTRALIZED: {
                "initial_reconstruction_timeout_milliseconds": 100,
                "num_heartbeats_timeout": 10,
                "object_manager_repeated_push_delay_ms": 1000,
                },
            BY_VALUE: {
                "record_ref_creation_sites":0,
                "initial_reconstruction_timeout_milliseconds": 100,
                "num_heartbeats_timeout": 10,
                "lineage_pinning_enabled": 1,
                "free_objects_period_milliseconds": -1,
                "object_manager_repeated_push_delay_ms": 1000,
                "task_retry_delay_ms": 100,
                "max_direct_call_object_size": 10737418240,
                "max_grpc_message_size": -1,
                },
            }
    config = configs[args.system]
    internal_config = json.dumps(config)
    address = "auto"
    if args.system in [LEASES, CENTRALIZED]:
        ray.init(address=address)
    else:
        ray.init(address=address, _internal_config=internal_config, redis_password='5241590000000000')

    num_nodes = 1
    nodes = [node for node in ray.nodes() if node["Alive"]]
    while len(nodes) < num_nodes + 1:
        time.sleep(1)
        print("{} nodes found, waiting for nodes to join".format(len(nodes)))
        nodes = [node for node in ray.nodes() if node["Alive"]]

    import socket
    ip_addr = socket.gethostbyname(socket.gethostname())
    head_resource = "node:{}".format(ip_addr)

    assert len(nodes) == 2
    for node in nodes:
        if args.local:
            if head_resource in node["Resources"]:
                print("Setting resource on node", node["NodeManagerAddress"])
                ray.experimental.set_resource(RESOURCE, 1, node["NodeID"])
            else:
                ray.experimental.set_resource(RESOURCE, 0, node["NodeID"])
        else:
            if head_resource in node["Resources"]:
                ray.experimental.set_resource(RESOURCE, 0, node["NodeID"])
            else:
                print("Setting resource on node", node["NodeManagerAddress"])
                ray.experimental.set_resource(RESOURCE, 1, node["NodeID"])


    print("All nodes joined")

    latencies = []
    for _ in range(args.num_trials):
        if args.use_actors:
            if args.pipelined:
                latencies += run_with_actors_pipelined(NUM_ITERATIONS)
            else:
                latencies += run_with_actors_sync(NUM_ITERATIONS)
        else:
            if args.pipelined:
                latencies += run_pipelined(NUM_ITERATIONS)
            else:
                latencies += run_sync(NUM_ITERATIONS)

        print("Latency", np.mean(latencies))

    if args.output:
        file_exists = False
        try:
            os.stat(args.output)
            file_exists = True
        except:
            pass

        with open(args.output, 'a+') as csvfile:
            fieldnames = ['system', 'local', 'actors', 'pipelined', 'latency']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()
            for latency in latencies:
                writer.writerow({
                    'system': args.system,
                    'local': args.local,
                    'actors': args.use_actors,
                    'pipelined': args.pipelined,
                    'latency': latency,
                    })

    if args.timeline:
        ray.timeline(filename=args.timeline)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--pipelined", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--use-actors", action="store_true")
    parser.add_argument("--timeline", type=str, default=None)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--num-trials", type=int, default=3)
    parser.add_argument("--system", type=str, required=True)
    args = parser.parse_args()

    if args.system not in [OWNERSHIP, LEASES, CENTRALIZED, BY_VALUE]:
        raise ValueError("--system must be one of {}".format([OWNERSHIP, LEASES, CENTRALIZED, BY_VALUE]))
    main(args)
