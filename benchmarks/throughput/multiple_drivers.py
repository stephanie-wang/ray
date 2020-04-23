import argparse
import json
import os
import socket
import time

import ray

import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument(
    "--arg-size", type=str, required=True, help="'small' or 'large'")

NUM_DRIVERS = 4
CHAIN_LENGTH = 100
SMALL_ARG = None
LARGE_ARG = np.zeros(1 * 1024 * 1024, dtype=np.uint8)  # 1 MiB
TASKS_PER_NODE_PER_BATCH = 100

def get_node_ids():
    my_ip = ".".join(socket.gethostname().split("-")[1:])
    node_ids = set()
    head_node_id = None
    for resource in ray.available_resources():
        if "node" in resource:
            if my_ip in resource:
                head_node_id = resource
            else:
                node_ids.add(resource)
    return head_node_id, node_ids


def do_batch(f, opts, node_ids, args=None):
    if args is None:
        args = {}
        for node_id in node_ids:
            args[node_id] = [opts.arg] * TASKS_PER_NODE_PER_BATCH

    results = {}
    for node_id in node_ids:
        results[node_id] = [f.options(resources={node_id: 0.0001}).remote(*args[node_id]) for _ in range(TASKS_PER_NODE_PER_BATCH)]

    return results


@ray.remote
def f(arg):
    return arg


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
    for _ in range(0):
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

    head_node_id, node_ids = get_node_ids()
    print("head:", head_node_id)
    print("workers:", node_ids)

    def do_chain():
        prev = None
        for _ in range(CHAIN_LENGTH):
            prev = do_batch(f, opts, node_ids, args=prev)

        all_oids = []
        for oids in prev.values():
            all_oids.extend(oids)

        ray.get(all_oids)

    timeit(do_chain, multiplier=len(node_ids) * TASKS_PER_NODE_PER_BATCH * CHAIN_LENGTH)


if __name__ == "__main__":
    args = parser.parse_args()
    if args.arg_size == "small":
        args.arg = SMALL_ARG
    elif args.arg_size == "large":
        args.arg = LARGE_ARG
    else:
        assert False
    main(args)
