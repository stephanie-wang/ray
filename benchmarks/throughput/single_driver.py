import argparse
import json
import os
import time

import ray

import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument(
    "--arg-size", type=str, required=True, help="'small' or 'large'")

SMALL_ARG_SIZE = 10 * 1024  # 10 KiB
LARGE_ARG_SIZE = 1024 * 1024  # 1 MiB
TASKS_PER_BATCH = 20


def do_batch(f, opts, args=None):
    if args is None:
        args = [
            np.zeros(opts.arg_size, dtype=np.uint8)
            for _ in range(TASKS_PER_BATCH)
        ]
    results = [f.remote(arg) for arg in args]
    ray.get(results)
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


def timeit(fn, multiplier=1):
    # warmup
    start = time.time()
    prev = None
    while time.time() - start < 5:
        prev = fn(prev)
    # real run
    stats = []
    trials = 4
    for i in range(trials):
        print("trial {}/{}".format(i + 1, trials))
        start = time.time()
        count = 0
        prev = None
        while time.time() - start < 10:
            prev = fn(prev)
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))
        print("finished", count)
    print("per second", round(np.mean(stats), 2), "+-", round(
        np.std(stats), 2))


def main(opts):
    do_ray_init(opts)

    def chained_batch(prev_batch):
        return do_batch(f, opts, args=prev_batch)

    timeit(chained_batch, multiplier=TASKS_PER_BATCH)


if __name__ == "__main__":
    args = parser.parse_args()
    if args.arg_size == "small":
        args.arg_size = SMALL_ARG_SIZE
    elif args.arg_size == "large":
        args.arg_size = LARGE_ARG_SIZE
    else:
        assert False
    main(args)
