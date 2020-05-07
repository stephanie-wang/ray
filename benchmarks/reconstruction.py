import cv2
import time
import json
import numpy as np
import threading
from collections import defaultdict
from ray import profiling
import os

import ray
import ray.cluster_utils

RESOURCE = "reconstruction"
JOB_TIME = 20


@ray.remote(resources={RESOURCE: 1})
def chain(dep):
    return dep

@ray.remote(resources={RESOURCE: 1})
def small_dep():
    return 1

@ray.remote(resources={RESOURCE: 1})
def large_dep():
    return np.zeros(1 * 1024 * 1024, dtype=np.uint8)  # 1 MiB

def run(num_chains, large):
    f = small_dep
    if large:
        f = large_dep
    chains = [f.remote() for i in range(num_chains)]
    start = time.time()
    i = 0
    while True:
        chains = [chain.remote(dep) for dep in chains]
        i += 1
        if i % 1000 == 0:
            ray.get(chains)
            print("Iteration", i)
        if time.time() - start > JOB_TIME:
            break
    return i

def main(args):
    if args.v07:
        config = {
            "initial_reconstruction_timeout_milliseconds": 100,
            "num_heartbeats_timeout": 10,
            "object_manager_repeated_push_delay_ms": 1000,
        }
    else:
        config = {
            "initial_reconstruction_timeout_milliseconds": 100,
            "num_heartbeats_timeout": 10,
            "lineage_pinning_enabled": 1,
            "free_objects_period_milliseconds": -1,
            "object_manager_repeated_push_delay_ms": 1000,
            "task_retry_delay_ms": 100,
        }


    internal_config = json.dumps(config)
    if args.local:
        cluster = ray.cluster_utils.Cluster()
        cluster.add_node(
            num_cpus=0, _internal_config=internal_config, include_webui=False,
            resources={"head": 100})
        for _ in range(args.num_nodes):
            cluster.add_node(
                object_store_memory=10**9,
                num_cpus=2,
                _internal_config=internal_config)
        cluster.wait_for_nodes()
        address = cluster.address
    else:
        address = "auto"

    if args.v07:
        ray.init(address=address)
    else:
        ray.init(address=address, _internal_config=internal_config, redis_password='5241590000000000')

    nodes = ray.nodes()
    while len(nodes) < args.num_nodes + 1:
        time.sleep(1)
        print("{} nodes found, waiting for nodes to join".format(len(nodes)))
        nodes = ray.nodes()

    for node in nodes:
        for resource in node["Resources"]:
            ray.experimental.set_resource(RESOURCE, 0, node["NodeID"])

    worker_ip = None
    head_ip = None
    if not args.local:
        import socket
        ip_addr = socket.gethostbyname(socket.gethostname())
        node_resource = "node:{}".format(ip_addr)

        for node in nodes:
            if node_resource in node["Resources"]:
                head_ip = node["NodeManagerAddress"]

    nodes = ray.nodes()
    i = 0
    for node in nodes:
        if "CPU" in node["Resources"]:
            worker_ip = node["NodeManagerAddress"]
            ray.experimental.set_resource(RESOURCE, 100, node["NodeID"])
            print("{}:{}".format(node["NodeManagerAddress"], node["NodeManagerPort"]))
            i += 1
            if i == args.num_nodes:
                break

    print("All nodes joined")

    if args.failure:
        if args.local:
            t = threading.Thread(
                target=run, args=(args.num_nodes, args.large))
            t.start()

            if args.failure:
                time.sleep(10)
                cluster.remove_node(cluster.list_all_nodes()[-1], allow_graceful=False)
            t.join()

            return
        else:
            print("Killing", worker_ip, "with resource", resource, "after 10s")
            def kill():
                cmd = 'ssh -i ~/ray_bootstrap_key.pem -o StrictHostKeyChecking=no {} "bash -s" -- < benchmarks/restart.sh {}'.format(worker_ip, head_ip)
                print(cmd)
                time.sleep(10)
                os.system(cmd)
            t = threading.Thread(target=kill)
            t.start()
            num_rounds = run(args.num_nodes, args.large)
            t.join()
    else:
        num_rounds = run(args.num_nodes, args.large)

    num_tasks = num_rounds * args.num_nodes
    print("Completed", num_tasks)
    print("Throughput", num_tasks / JOB_TIME)


    if args.timeline:
        ray.timeline(filename=args.timeline)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--num-nodes", required=True, type=int)
    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--large", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--timeline", type=str, default=None)
    parser.add_argument("--v07", action="store_true")
    args = parser.parse_args()
    main(args)
