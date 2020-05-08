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

RESOURCE = "reconstruction"
JOB_TIME = 10


@ray.remote(resources={RESOURCE: 1})
def chain(i, dep, delay_ms):
    print(i)
    time.sleep(delay_ms / 1000.0)
    return dep

@ray.remote(resources={RESOURCE: 1})
def small_dep():
    return 1

@ray.remote(resources={RESOURCE: 1})
def large_dep():
    return np.zeros(1 * 1024 * 1024, dtype=np.uint8)  # 1 MiB

def run(delay_ms, large, num_rounds):
    f = small_dep
    if large:
        f = large_dep
    start = time.time()
    dep = f.remote()
    for i in range(num_rounds):
        dep = chain.remote(i, dep, delay_ms)
    ray.get(dep)
    return time.time() - start

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


    num_nodes = 2
    internal_config = json.dumps(config)
    if args.local:
        cluster = ray.cluster_utils.Cluster()
        cluster.add_node(
            num_cpus=0, _internal_config=internal_config, include_webui=False,
            resources={"head": 100})
        for _ in range(num_nodes):
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
    while len(nodes) < num_nodes + 1:
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
    for node in nodes:
        if "CPU" in node["Resources"]:
            worker_ip = node["NodeManagerAddress"]
            ray.experimental.set_resource(RESOURCE, 100, node["NodeID"])
            print("{}:{}".format(node["NodeManagerAddress"], node["NodeManagerPort"]))
            break

    print("All nodes joined")

    num_rounds = int(JOB_TIME / (args.delay_ms / 1000))
    print("Running", num_rounds, "rounds of", args.delay_ms, "ms each")

    if args.failure:
        if args.local:
            t = threading.Thread(
                target=run, args=(args.delay_ms, args.large))
            t.start()

            if args.failure:
                time.sleep(JOB_TIME)
                cluster.remove_node(cluster.list_all_nodes()[-1], allow_graceful=False)
            t.join()

            return
        else:
            print("Failure halfway")
            sleep = JOB_TIME / 2
            print("Killing", worker_ip, "with resource", resource, "after {}s".format(sleep))
            def kill():
                if args.v07:
                    script = "benchmarks/restart_0_7.sh"
                else:
                    script = "benchmarks/restart.sh"
                cmd = 'ssh -i ~/ray_bootstrap_key.pem -o StrictHostKeyChecking=no {} "bash -s" -- < {} {}'.format(worker_ip, script, head_ip)
                print(cmd)
                time.sleep(sleep)
                os.system(cmd)
                for node in ray.nodes():
                    if "CPU" in node["Resources"] and node["NodeManagerAddress"] != worker_ip:
                        ray.experimental.set_resource(RESOURCE, 100, node["NodeID"])
                        print("Restarted node at {}:{}".format(node["NodeManagerAddress"], node["NodeManagerPort"]))
                        break

            t = threading.Thread(target=kill)
            t.start()
            duration = run(args.delay_ms, args.large, num_rounds)
            t.join()
    else:
        duration = run(args.delay_ms, args.large, num_rounds)

    print("Task delay", args.delay_ms, "ms. Duration", duration)

    if args.output:
        file_exists = False
        try:
            os.stat(args.output)
            file_exists = True
        except:
            pass

        with open(args.output, 'a+') as csvfile:
            fieldnames = ['ownership', 'large', 'delay_ms', 'duration', 'failure']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()
            writer.writerow({
                'ownership': not args.v07,
                'large': args.large,
                'delay_ms': args.delay_ms,
                'duration': duration,
                'failure': args.failure,
                })

    if args.timeline:
        ray.timeline(filename=args.timeline)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--large", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--timeline", type=str, default=None)
    parser.add_argument("--v07", action="store_true")
    parser.add_argument("--delay-ms", required=True, type=int)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()
    main(args)
