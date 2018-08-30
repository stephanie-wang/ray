from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import datetime
import time
import argparse
import numpy as np

BATCH_SIZE = 100

@ray.remote
def pong(submit_time):
    return time.time() - submit_time

class A(object):
    def __init__(self, ping_node_resource, pong_node_resource, group_size):
        print("Actor A start...")
        actor_cls = ray.remote(resources={ping_node_resource: 1})(B)
        self.b = [actor_cls.remote(pong_node_resource) for _ in
                range(group_size)]
        ray.get([b.ready.remote() for b in self.b])

    def ready(self):
        time.sleep(1)
        return True

    def f(self, target_throughput, experiment_time):
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        start = time.time()
        start2 = time.time()
        i = 0
        batch_size = BATCH_SIZE
        if target_throughput < batch_size:
            batch_size = int(target_throughput)
        batch_size //= len(self.b)
        target_throughput //= len(self.b)
        while True:
            [b.ping.remote(time.time()) for b in self.b]
            if i % batch_size == 0 and i > 0:
                end = time.time()
                sleep_time = (batch_size / target_throughput) - (end - start2)
                if sleep_time > 0.00003:
                    time.sleep(sleep_time)
                start2 = time.time()
                if end - start >= experiment_time:
                    break
            i += 1

        latencies = ray.get([b.get_sum.remote() for b in self.b])
        latencies = [max(latency) for latency in zip(*latencies)]
        return latencies

class B(object):
    def __init__(self, node_resource):
        print("Actor B start...")
        self.latencies = []
        #import yep
        #yep.start('actorB.prof')
        self.node_resource = node_resource

    def ready(self):
        time.sleep(1)
        return True

    def ping(self, submit_time):
        self.latencies.append(pong._submit(
                args=[submit_time],
                resources={
                    self.node_resource: 1,
                    }))

    def get_sum(self):
        #import yep
        #yep.stop()
        return ray.get(self.latencies)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--target-throughput', type=int, default=1000)
    parser.add_argument('--experiment-time', type=int, default=10)
    parser.add_argument('--num-workers', type=int, default=1)
    parser.add_argument('--pingpong', action='store_true')
    parser.add_argument('--num-raylets', type=int, default=1)
    parser.add_argument('--num-shards', type=int, default=None)
    parser.add_argument('--gcs', action='store_true')
    parser.add_argument('--policy', type=int, default=0)
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--max-lineage-size', type=str)
    parser.add_argument('--group-size', type=int, default=1)
    parser.add_argument('--sample-remote', action='store_true')
    parser.add_argument('--sample-local', action='store_true')
    args = parser.parse_args()

    if args.pingpong:
        args.target_throughput //= 2

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
                         num_workers=args.num_workers,
                         max_lineage_size=args.max_lineage_size)
    else:
        ray.init(redis_address=args.redis_address + ":6379", use_raylet=True)

    actors = []
    for node in range(args.num_raylets):
        for _ in range(args.num_workers):
            send_resource = "Node{}".format(node * 2)
            receive_resource = "Node{}".format(node * 2 + 1)

            if args.sample_local and node == args.num_raylets - 1:
                receive_resource = send_resource

            actor_cls = ray.remote(resources={
                send_resource: 1,
                })(A)
            actors.append(actor_cls.remote(receive_resource, send_resource, args.group_size))

            if args.pingpong:
                actor_cls = ray.remote(resources={
                    receive_resource: 1
                    })(A)
                actors.append(actor_cls.remote(send_resource, receive_resource, args.group_size))

    total_time = ray.get([actor.ready.remote() for actor in actors])
    if args.sample_remote or args.sample_local:
        for actor in actors[:-1]:
            actor.f.remote(args.target_throughput, args.experiment_time)
        latencies = ray.get([actors[-1].f.remote(10, args.experiment_time)])
    else:
        latencies = ray.get([actor.f.remote(
            args.target_throughput,
            args.experiment_time) for actor in actors])
    for latency in latencies:
        if args.sample_remote or args.sample_local:
            line = [
                gcs_delay_ms,
                min(latency),
                args.num_shards,
                args.target_throughput
                ]
            if args.sample_local:
                line.append(0)
            else:
                line.append(1)
            print(','.join([str(field) for field in line]))
        else:
            print("DONE, average latency:", sum(latency) / len(latency))
            for point in latency:
                print(point)
