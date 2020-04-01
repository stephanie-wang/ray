import argparse
import json
import os
import time

import ray

import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--connect", default=False, action="store_true")
parser.add_argument("--chained", default=False, action="store_true")
parser.add_argument("--centralized", default=False, action="store_true")
parser.add_argument("--tasks-per-batch", type=int, default=10)
parser.add_argument(
    "--arg-size", type=str, required=True, help="'small' or 'large'")
parser.add_argument("--num-args", type=int, default=1)
parser.add_argument("--tree", default=False, action="store_true")

SMALL_ARG_SIZE = 10 * 1024  # 10 KiB
LARGE_ARG_SIZE = 1024 * 1024  # 1 MiB
TREE_DEPTH = 2


def do_batch(f, opts, args=None, depth=0):
    if args is None:
        args = [
            np.zeros(opts.arg_size, dtype=np.uint8)
            for _ in range(opts.num_args)
        ]
    return ray.get([
        f.remote(opts, *args, depth=depth) for _ in range(opts.tasks_per_batch)
    ])


@ray.remote(max_retries=0)
def f(opts, *args, depth=0):
    if not opts.tree or depth == TREE_DEPTH:
        return args
    return do_batch(f, opts, depth=depth + 1)


def do_ray_init(args):
    internal_config = {"centralized_owner": int(args.centralized)}
    if os.environ.get("BY_VAL_ONLY", False):
        # Set threshold to 1 TiB to force everything to be inlined.
        internal_config["max_direct_call_object_size"] = 1024**4

    address = None
    if args.connect:
        address = "auto"

    print("Starting ray with:", internal_config)
    ray.init(address=address, _internal_config=json.dumps(internal_config))


def timeit(fn, multiplier=1):
    # warmup
    start = time.time()
    prev = None
    while time.time() - start < 1:
        prev = fn(prev)
    # real run
    stats = []
    trials = 4
    for i in range(trials):
        print("trial {}/{}".format(i + 1, trials))
        start = time.time()
        count = 0
        prev = None
        while time.time() - start < 2:
            prev = fn(prev)
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))
    print("per second", round(np.mean(stats), 2), "+-", round(
        np.std(stats), 2))


def main(opts):
    do_ray_init(opts)

    def chained_batch(prev_batch):
        args = None
        if opts.chained and prev_batch is not None:
            args = prev_batch
        return do_batch(f, opts, args=args)

    multiplier = opts.tasks_per_batch
    if args.tree:
        multiplier = multiplier**TREE_DEPTH
    timeit(chained_batch, multiplier=multiplier)


if __name__ == "__main__":
    args = parser.parse_args()
    if args.arg_size == "small":
        args.arg_size = SMALL_ARG_SIZE
    elif args.arg_size == "large":
        args.arg_size = LARGE_ARG_SIZE
    main(args)
