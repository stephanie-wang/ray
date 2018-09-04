from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import datetime
import time
import argparse
import subprocess

BATCH_SIZE = 100
ROUND_TIME = 0.25

@ray.remote
def foo():
    return 1

def stop_node(local):
    if local:
        p = ray.services.all_processes[ray.services.PROCESS_TYPE_RAYLET][-1]
        ray.services.kill_process(p)
    else:
        with open("/home/ubuntu/ray/benchmark/cluster-scripts/workers.txt", 'r') as f:
            for line in f.readlines():
                node = line.strip()
        print("Killing node", node)
        command = [
                "ssh",
                '-o', 'StrictHostKeyChecking=no',
                "-i", "/home/ubuntu/devenv-key.pem",
                node,
                "PATH=/home/ubuntu/anaconda3/bin/:$PATH", "ray", "stop",
                ]
        print(command)
        with open("/tmp/worker.out", 'a+') as f:
            subprocess.Popen(command, stdout=f, stderr=f)

class A(object):
    def __init__(self, node_resource):
        print("Actor A start...")
        self.node_resource = node_resource

    def ready(self):
        time.sleep(1)
        return True

    def f(self, target_throughput, experiment_time, test_failure, local):
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push, start " + time_str)
        i = 0
        batch_size = BATCH_SIZE
        if target_throughput < batch_size:
            batch_size = int(target_throughput)
        batch = []
        latencies = []
        round_start = time.time()
        batch_start = time.time()
        round_number = 0
        start = time.time()
        while True:
            batch.append(foo._submit(args=[], resources={self.node_resource: 1}))
            if i % batch_size == 0 and i > 0:
                end = time.time()
                if end - round_start > ROUND_TIME:
                    print("Getting round", round_number)
                    ray.get(batch)
                    latencies.append((len(batch), time.time() - round_start))
                    batch = []
                    round_number += 1
                    if time.time() - start >= experiment_time / 2 and test_failure:
                        stop_node(local)
                        test_failure = True
                    if time.time() - start >= experiment_time:
                        break

                    round_start = time.time()
                    batch_start = time.time()

                end = time.time()
                sleep_time = (batch_size / target_throughput) - (end - batch_start)
                if sleep_time > 0.00003:
                    time.sleep(sleep_time)
                batch_start = time.time()

            i += 1

        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push, end " + time_str)

        return latencies


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--target-throughput', type=int, default=3000)
    parser.add_argument('--experiment-time', type=int, default=10)
    parser.add_argument('--num-workers', type=int, default=1)
    parser.add_argument('--pingpong', action='store_true')
    parser.add_argument('--num-raylets', type=int, default=1)
    parser.add_argument('--num-shards', type=int, default=None)
    parser.add_argument('--gcs', action='store_true')
    parser.add_argument('--policy', type=int, default=0)
    parser.add_argument('--max-lineage-size', type=int, default=None)
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--failure', action='store_true')
    args = parser.parse_args()

    if args.pingpong:
        args.num_workers //= 2

    gcs_delay_ms = -1
    if args.gcs:
        gcs_delay_ms = 0

    if args.redis_address is None:
        ray.worker._init(start_ray_local=True, use_raylet=True, num_local_schedulers=args.num_raylets * 2,
                         resources=[
                            {
                                "Node{}".format(i): args.num_workers,
                            } for i in range(args.num_raylets * 2)],
                         gcs_delay_ms=gcs_delay_ms,
                         num_redis_shards=args.num_shards,
                         lineage_cache_policy=args.policy,
                         max_lineage_size=args.max_lineage_size)
    else:
        ray.init(redis_address=args.redis_address + ":6379", use_raylet=True)

    actors = []
    for node in range(args.num_raylets):
        for _ in range(args.num_workers):
            send_resource = "Node{}".format(node * 2)
            receive_resource = "Node{}".format(node * 2 + 1)
            actor_cls = ray.remote(resources={
                send_resource: 1,
                })(A)
            actors.append(actor_cls.remote(receive_resource))

            if args.pingpong:
                actor_cls = ray.remote(resources={
                    receive_resource: 1
                    })(A)
                actors.append(actor_cls.remote(send_resource))
    ray.get([actor.ready.remote() for actor in actors])
    all_results = [actor.f.remote(args.target_throughput,
                                  args.experiment_time,
                                  args.failure,
                                  args.redis_address is None) for actor in actors]
    all_results = ray.get(all_results)
    for results in all_results:
        for i, result in enumerate(results):
            print(i, result[0], result[1])
