import ray
import logging
import time
import numpy as np


logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

NUM_TRIALS = 30


@ray.remote
def tree(n, sleep_time_ms):
    time.sleep(float(sleep_time_ms) / 1000)

    if n <= 1:
        return n

    # Call ray.put on the argument to create a dependency on the parent task.
    child = ray.put(n / 2)
    nodes = ray.get([
        tree.remote(child, sleep_time_ms),
        tree.remote(child, sleep_time_ms),
    ])

    return nodes[0] + nodes[1]


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-raylet', action='store_true')
    parser.add_argument('--no-hugepages', action='store_true')
    parser.add_argument('--num-recursions', type=int, required=True)
    parser.add_argument('--num-nodes', type=int)
    parser.add_argument('--task-delay-ms', type=int, default=1)
    parser.add_argument('--gcs-delay-ms', type=int)
    parser.add_argument('--redis-address', type=str)

    args = parser.parse_args()
    num_tasks = 2 ** args.num_recursions

    if args.redis_address is None:
        num_initial_workers = min(4 * args.num_recursions * args.num_nodes, 500)

        huge_pages = not args.no_hugepages
        if huge_pages:
            plasma_directory = "/mnt/hugepages"
        else:
            plasma_directory = None

        ray.worker._init(
                start_ray_local=True,
                redirect_output=False,
                use_raylet=args.use_raylet,
                num_local_schedulers=args.num_nodes,
                num_cpus=4,
                num_workers=num_initial_workers,
                gcs_delay_ms=args.gcs_delay_ms if args.gcs_delay_ms is not None else -1,
                huge_pages=huge_pages,
                plasma_directory=plasma_directory,
                use_task_shard=True
                )
    else:
        ray.init(
                redis_address=args.redis_address,
                use_raylet=args.use_raylet)

    x = np.ones(10 ** 8)
    for _ in range(100):
        ray.put(x)


    for _ in range(NUM_TRIALS):
        time.sleep(1)

        start = time.time()
        ray.get(tree.remote(num_tasks, args.task_delay_ms))
        log.info("Ray took %f seconds", time.time() - start)
