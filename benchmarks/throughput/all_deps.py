import argparse
import json
import os
import random
import socket
import time
import datetime
from collections import defaultdict

import ray

import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument(
    "--arg-size", type=str, required=True, help="'small' or 'large'")
parser.add_argument(
    "--num-nodes", type=int, required=True, help="Number of nodes in the cluster")
parser.add_argument(
    "--no-args", action="store_true", help="Submit tasks with no arguments")
parser.add_argument(
    "--sharded", action="store_true", help="Whether to shard the driver")
parser.add_argument(
    "--timeline", action="store_true", help="Whether to dump a timeline")

CHAIN_LENGTH = 1000
SMALL_ARG = lambda: None
LARGE_ARG = lambda: np.zeros(1 * 1024 * 1024, dtype=np.uint8)  # 1 MiB
TASKS_PER_NODE_PER_BATCH = 10

def get_node_ids():
    my_ip = ".".join(socket.gethostname().split("-")[1:])
    node_ids = set()
    for resource in ray.available_resources():
        if "node" in resource and not my_ip in resource:
            node_ids.add(resource)
    return node_ids


def get_local_node_resource():
    my_ip = ".".join(socket.gethostname().split("-")[1:])
    addr = "node:{}".format(my_ip)
    return addr


@ray.remote
def f(use_small, *args):
    if use_small:
        return SMALL_ARG()
    else:
        return LARGE_ARG()


def do_batch(use_small, no_args, node_ids, args=None):
    if args is None:
        args = {}
        for node_id in node_ids:
            args[node_id] = []

    results = defaultdict(list)
    for _ in range(TASKS_PER_NODE_PER_BATCH):
        for node_id in node_ids:
            if no_args:
                results[node_id].append(f.options(resources={node_id: 0.0001}).remote(use_small))
            else:
                results[node_id].append(f.options(resources={node_id: 0.0001}).remote(use_small, *args[node_id]))

    return results


def do_ray_init(arg):
    internal_config = {"record_ref_creation_sites": 0}
    if os.environ.get("CENTRALIZED", False):
        internal_config["centralized_owner"] = 1
    if os.environ.get("BY_VAL_ONLY", False):
        # Set threshold to 1 TiB to force everything to be inlined.
        internal_config["max_direct_call_object_size"] = 1024**4

    internal_config = json.dumps(internal_config)
    if os.environ.get("RAY_0_7", False):
        internal_config = None

    print("Starting ray with:", internal_config)
    ray.init(address="auto", _internal_config=internal_config)


def timeit(fn, trials=1, multiplier=1):
    start = time.time()
    for _ in range(1):
        start = time.time()
        fn()
        print("finished warmup iteration in", time.time()-start)

    stats = []
    for i in range(trials):
        start = time.time()
        fn()
        end = time.time()
        print("finished {}/{} in {}".format(i+1, trials, end-start))
        stats.append(multiplier / (end - start))
    print("per second", round(np.mean(stats), 2), "+-", round(
        np.std(stats), 2))


def main(opts):
    do_ray_init(opts)

    node_ids = get_node_ids()
    while len(node_ids) < opts.num_nodes:
        print("Not all nodes have joined yet, sleeping for 1s...", time.sleep(1))
        node_ids = get_node_ids()
    node_ids = list(node_ids)[:opts.num_nodes]
    print("All {} nodes joined: {}".format(len(node_ids), node_ids))

    def do_chain(node_ids, use_small, no_args):
        prev = None
        for _ in range(CHAIN_LENGTH):
            prev = do_batch(use_small, no_args, node_ids, args=prev)

        all_oids = []
        for oids in prev.values():
            all_oids.extend(oids)

        ray.get(all_oids)


    use_small = opts.arg_size == "small"
    if opts.sharded:
        do_chain = ray.remote(do_chain)
        def job():
            drivers = []
            for node_id in node_ids:
                drivers.append(do_chain.options(num_cpus=0, resources={get_local_node_resource(): 0.0001}).remote({node_id}, use_small, opts.no_args))
            ray.get(drivers)
    else:
        def job():
            do_chain(node_ids, use_small, opts.no_args)

    timeit(job, multiplier=len(node_ids) * TASKS_PER_NODE_PER_BATCH * CHAIN_LENGTH)

    if opts.timeline:
        now = datetime.datetime.now()
        ray.timeline(filename="dump {}.json".format(now))


if __name__ == "__main__":
    args = parser.parse_args()
    assert args.arg_size == "small" or args.arg_size == "large"
    main(args)
