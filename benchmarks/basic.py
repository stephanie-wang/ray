import argparse
import json
import time

import ray

parser = argparse.ArgumentParser()
parser.add_argument("--centralized", default=False, action="store_true")
parser.add_argument(
    "--no-centralized", dest="centralized", action="store_false")


@ray.remote
def f(*args):
    pass


def main(args):
    ray.init(
        _internal_config=json.dumps({
            "centralized_owner": int(args.centralized)
        }))

    times = []
    arg = ray.put(0)
    for i in range(1100):
        start = time.time()
        ray.get([f.remote(arg) for _ in range(100)])
        if i > 100:
            times.append(time.time() - start)

    print("avg:", sum(times) / len(times))


if __name__ == "__main__":
    main(parser.parse_args())
