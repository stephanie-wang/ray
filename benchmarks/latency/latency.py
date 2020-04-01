import argparse
import json
import os
import time

import ray

import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--centralized", default=False, action="store_true")
parser.add_argument(
    "--arg-size", type=str, required=True, help="'small' or 'large'")

SMALL_ARG_SIZE = 10 * 1024  # 10 KiB
LARGE_ARG_SIZE = 1024 * 1024  # 1 MiB
TREE_DEPTH = 2


# Worker nodes only have 4 CPUs, force spread.
@ray.remote(num_cpus=4)
class Actor2:
    def __init__(self, other):
        self.other = other

    def ping(self, arg):
        self.other.pong.remote(arg)


# Worker nodes only have 4 CPUs, force spread.
@ray.remote(num_cpus=4)
class Actor1:
    def __init__(self):
        self.rtts = []

    def do_ping_pong(self, other, arg):
        self.start_time = time.time()
        other.ping.remote(arg)

    def pong(self, arg):
        self.rtts.append(time.time() - self.start_time)

    def get_rtts(self):
        return self.rtts


def do_ray_init(args):
    internal_config = {"centralized_owner": int(args.centralized)}
    if os.environ.get("BY_VAL_ONLY", False):
        # Set threshold to 1 TiB to force everything to be inlined.
        internal_config["max_direct_call_object_size"] = 1024**4

    print("Starting ray with:", internal_config)
    ray.init(address="auto", _internal_config=json.dumps(internal_config))


def main(opts):
    do_ray_init(opts)

    actor1 = Actor1.remote()
    actor2 = Actor2.remote(actor1)
    arg = np.zeros(opts.arg_size, dtype=np.uint8)
    trials = 10
    for i in range(trials):
        print("iter {}/{}".format(i + 1, trials))
        time.sleep(1)
        actor1.do_ping_pong.remote(actor2, arg)

    latencies = [rtt / 2 for rtt in ray.get(actor1.get_rtts.remote())]
    print("avg:", sum(latencies) / len(latencies))


if __name__ == "__main__":
    args = parser.parse_args()
    if args.arg_size == "small":
        args.arg_size = SMALL_ARG_SIZE
    elif args.arg_size == "large":
        args.arg_size = LARGE_ARG_SIZE
    main(args)
