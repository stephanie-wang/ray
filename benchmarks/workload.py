import argparse
import json
import time

import ray

import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--chained", default=False, action="store_true")
parser.add_argument("--centralized", default=False, action="store_true")
parser.add_argument("--no-by-ref", default=False, action="store_true")
parser.add_argument("--tasks-per-batch", type=int, required=True)
parser.add_argument("--num-args", type=int, required=True)
parser.add_argument("--arg-size", type=int, required=True)
parser.add_argument("--iterations", type=int, required=True)
parser.add_argument("--max-depth", default=0, type=int)

WARM_UP_ITERATIONS = 100


def do_batch(f, opts, args=None):
    if args is None:
        args = [
            np.zeros(opts.arg_size, dtype=np.uint8)
            for _ in range(opts.num_args)
        ]
    return ray.get(
        [f.remote(opts, *args) for _ in range(opts.tasks_per_batch)])


@ray.remote
def f(opts, *args, depth=0):
    if depth == opts.max_depth:
        return args
    return do_batch(opts, depth=depth + 1)


def do_ray_init(args):
    internal_config = {"centralized_owner": int(args.centralized)}
    if args.no_by_ref:
        # Set threshold to 1 TiB to force everything to be inlined.
        internal_config["max_direct_call_object_size"] = 1024**4

    print("Starting ray with:", internal_config)
    ray.init(_internal_config=json.dumps(internal_config))


def main(opts):
    do_ray_init(opts)

    times = []
    prev_batch = None
    for i in range(opts.iterations + WARM_UP_ITERATIONS):
        start = time.time()
        args = None
        if opts.chained and prev_batch is not None:
            args = prev_batch
        prev_batch = do_batch(f, opts, args=args)
        if i > WARM_UP_ITERATIONS:
            times.append(time.time() - start)

    print("avg:", sum(times) / len(times))


if __name__ == "__main__":
    main(parser.parse_args())
