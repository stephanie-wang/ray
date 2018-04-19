import ray
import logging
import time


logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

NUM_TRIALS = 10


@ray.remote
def tree(n, sleep_time_ms):
    time.sleep(float(sleep_time_ms) / 1000)

    if n <= 1:
        return n

    nodes = ray.get([
        tree.remote(n / 2, sleep_time_ms),
        tree.remote(n / 2, sleep_time_ms),
    ])

    return nodes[0] + nodes[1]


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-raylet', action='store_true')
    parser.add_argument('--num-recursions', type=int, required=True)
    parser.add_argument('--num-nodes', type=int, required=True)
    parser.add_argument('--task-delay', type=int, default=0)
    parser.add_argument('--gcs-delay', type=int)

    args = parser.parse_args()

    num_tasks = args.num_nodes * args.num_recursions
    num_initial_workers = min(num_tasks, 500)

    ray.worker._init(
            start_ray_local=True,
            use_raylet=args.use_raylet,
            redirect_output=False,
            num_local_schedulers=args.num_nodes,
            num_cpus=4,
            num_workers=num_initial_workers,
            gcs_delay_ms=args.gcs_delay if args.gcs_delay is not None else -1
            )
    time.sleep(30)

    for _ in range(NUM_TRIALS):
        time.sleep(1)

        start = time.time()
        ray.get(tree.remote(num_tasks, args.task_delay))
        log.info("Ray took %f seconds", time.time() - start)
