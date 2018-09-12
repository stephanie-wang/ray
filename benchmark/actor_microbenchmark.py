from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import datetime
import time
import argparse

BATCH_SIZE = 100
BATCH = False

class A(object):
    def __init__(self, node_resource):
        print("Actor A start...")
        self.b = ray.remote(resources={node_resource: 1})(B).remote()
        ray.get(self.b.ready.remote())

    def ready(self):
        time.sleep(1)
        return True

    def f(self, target_throughput, experiment_time):
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push, start " + time_str)
        start = time.time()
        start1 = time.time()
        start2 = time.time()
        i = 0
        batch_size = BATCH_SIZE
        if target_throughput < batch_size:
            batch_size = int(target_throughput)
        batch = []
        while True:
            if BATCH:
                batch.append(self.b.f.remote_batch())
            else:
                self.b.f.remote()
            if i % batch_size == 0 and i > 0:
                if BATCH:
                    ray.worker.global_worker.submit_batch(batch)
                    batch = []
                end = time.time()
                sleep_time = (batch_size / target_throughput) - (end - start2)
                if sleep_time > 0.00003:
                    print("Ahead by", sleep_time)
                    time.sleep(sleep_time)
                else:
                    print("Behind by", -1 * sleep_time)
                start2 = time.time()
                if end - start >= experiment_time:
                    break

            if i % 10000 == 0 and i > 0:
                end = time.time()
                print("push, throughput round ", i / 10000, ":", 10000 / (end - start1))
                start1 = end

            i += 1

        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push, end " + time_str)

        ray.get(self.b.get_sum.remote())
        end = time.time()
        return i, end - start

class B(object):
    def __init__(self):
        print("Actor B start...")
        self.sum = 0
        #import yep
        #yep.start('actorB.prof')

    def ready(self):
        time.sleep(1)
        return True

    def f(self):
        return

    def get_sum(self):
        #import yep
        #yep.stop()
        return self.sum


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
    results = ray.get([actor.f.remote(args.target_throughput,
                                      args.experiment_time) for actor in
                                      actors])
    num_items, total_time = zip(*results)
    throughput = sum(num_items) / max(total_time)
    print("DONE, target throughput:", args.target_throughput * len(actors), "total throughput:", throughput)
