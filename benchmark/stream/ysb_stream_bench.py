from __future__ import print_function
import argparse
from collections import defaultdict
from collections import Iterable
import time
import logging
import numpy as np
import redis
import ujson
import uuid
import sys
import subprocess
import os

import ray
import ray.cloudpickle as pickle


from conf import *
from generate import *

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

BATCH = True
USE_OBJECTS = True
SEPARATE_REDUCERS = True
CHECKPOINT_INTERVAL = 1  # Take a checkpoint every second.

CAMPAIGN_TO_PARTITION = {}

@ray.remote
def warmup_objectstore():
    pass  # return to skip warmup for faster testing
    x = np.ones(10 ** 8)
    for _ in range(100):
        ray.put(x)

@ray.remote
def warmup(*args):
    return

def ray_warmup(reducers, node_resources, num_reducer_nodes):
    """
    Warmup Ray
    """
    warmups = []
    for node_resource in node_resources:
        warmups.append(warmup_objectstore._submit(
            args=[],
            resources={node_resource: 1}))
    ray.wait(warmups, num_returns=len(warmups))

    last_node_resource = "Node{}".format(len(node_resources) - 1)
    gen_dep = init_generator._submit(
            args=[AD_TO_CAMPAIGN_MAP, time_slice_num_events],
            resources={
                last_node_resource: 1,
                })
    ray.get(gen_dep)

    num_rounds = 0
    for i in range(num_rounds):
        with ray.profiling.profile("reduce_round", worker=ray.worker.global_worker):
            start = time.time()
            if SEPARATE_REDUCERS:
                start_index = num_reducer_nodes
                args = [[[time.time()], gen_dep] for _ in range(start_index, len(node_resources))]
            else:
                args = [[[time.time()], gen_dep] for _ in range(len(node_resources))]
                start_index = 0
            if BATCH:
                for _ in range(5):
                    batch = []
                    for j, node_resource in enumerate(node_resources[start_index:]):
                        task = reduce_warmup._submit(
                                        args=args[j],
                                        resources={node_resource: 1},
                                        batch=True)
                        batch.append(task)
                    args = ray.worker.global_worker.submit_batch(batch)
                    args = [[arg] for arg in args]
                args = [arg[0] for arg in args]
                batch = [reducer.foo.remote_batch(*args) for reducer in reducers]
                ray.worker.global_worker.submit_batch(batch)
            else:
                for _ in range(5):
                    batch = []
                    for j, node_resource in enumerate(node_resources):
                        return_value = reduce_warmup._submit(
                                        args=[args[j]],
                                        resources={node_resource: 1})
                        batch.append(return_value)
                    args = batch
                [reducer.foo.remote(*args) for reducer in reducers]

        took = time.time() - start
        if took > 0.1:
            print("Behind by", took - 0.1)
        else:
            print("Ahead by", 0.1 - took)
            time.sleep(0.1 - took)
        if i % 10 == 0:
            print("finished warmup round", i, "out of", num_rounds)

    warmups = []
    for node_resource in reversed(node_resources):
        warmups.append(warmup._submit(
            args=[gen_dep],
            resources={node_resource: 1}))
    ray.wait(warmups, num_returns=len(warmups))
    return gen_dep


@ray.remote
def reduce_warmup(timestamp, *args):
    timestamp = timestamp[0]
    time.sleep(0.05)
    if USE_OBJECTS:
        return [timestamp for _ in range(1000)]
    else:
        return [timestamp]

def submit_tasks_no_json(gen_dep, num_reducer_nodes, window_size, batch_start_time):
    batch_round = [] # a list containing a round of generate, filter, project, reduce batches
    batch_round_tstamp = batch_start_time # batch timestamp for this round of task batches

    if SEPARATE_REDUCERS:
        start_index = num_reducer_nodes
    else:
        start_index = 0

    generated = []
    for i in range(start_index, num_nodes):
        for _ in range(num_generators_per_node):
            timestamp = (str(time.time())).encode('ascii')[:16]
            generated.append(generate._submit(
                args=[gen_dep, timestamp, time_slice_num_events],
                resources={node_resources[i]: 1},
                batch=BATCH))
    #if BATCH:
    #    generated = ray.worker.global_worker.submit_batch(generated)
    #generated = flatten(generated)

    filtered = []
    partition_size = time_slice_num_events // num_parsers_per_node
    for i, generate_output in zip(range(start_index, num_nodes), generated):
        generate_output = generate_output.returns()[0]
        partition_start = 0
        for j in range(num_parsers_per_node):
            partition_end = partition_start + partition_size
            if j < time_slice_num_events % num_parsers_per_node:
                partition_end += 1
            filtered.append(filter_no_json._submit(
                args=[num_filter_out, partition_start, partition_end, generate_output],
                num_return_vals=num_filter_out,
                resources={node_resources[i] : 1},
                batch=BATCH))
    if BATCH:
        # filtered = ray.worker.global_worker.submit_batch(generated + filtered)[len(generated):]
        # Append the generate and filtered batch to the container.
        batch_round.append(generated)
        batch_round.append(filtered)
        filtered = ray.worker.global_worker.submit_batch_preprocess(filtered)

    filtered = flatten(filtered)

    shuffled, start_idx = [], 0
    num_return_vals = 1
    for i in range(start_index, num_nodes):
        for _ in range(num_projectors_per_node):
            batches = filtered[start_idx : start_idx + num_filter_in]
            shuffled.append(project_shuffle_no_json._submit(
                args=[num_projector_out, window_size] + batches,
                num_return_vals=num_return_vals,
                resources={node_resources[i] : 1},
                batch=BATCH))
            start_idx += num_filter_in
    if BATCH:
        #shuffled = ray.worker.global_worker.submit_batch(shuffled)
        batch_round.append(shuffled)
        shuffled = ray.worker.global_worker.submit_batch_preprocess(shuffled)

    if BATCH:
        batch = [reducer.reduce.remote_batch(*shuffled) for reducer in reducers]
        #ray.worker.global_worker.submit_batch(batch)
        batch_round.append(batch)
    else:
        [reducer.reduce.remote(*shuffled) for reducer in reducers]

    # Prepare a batch of warmup tasks, if needed to trigger the push optimizations
    # batch = [warmup._submit(args=[], resources={node_resource: 1}, batch=True) for node_resource in node_resources]
    # ray.worker.global_worker.submit_batch(batch)
    return batch_round_tstamp, batch_round

def submit_tasks(gen_dep, num_reducer_nodes, window_size):
    if SEPARATE_REDUCERS:
        start_index = num_reducer_nodes
    else:
        start_index = 0

    generated = []
    for i in range(start_index, num_nodes):
        for _ in range(num_generators_per_node):
            timestamp = (str(time.time())).encode('ascii')[:16]
            generated.append(generate._submit(
                args=[gen_dep, timestamp, time_slice_num_events],
                resources={node_resources[i]: 1},
                batch=BATCH))
    if BATCH:
        generated = ray.worker.global_worker.submit_batch(generated)
    generated = flatten(generated)

    parsed, start_idx = [], 0
    partition_size = time_slice_num_events // num_parsers_per_node
    for i, generate_output in zip(range(start_index, num_nodes), generated):
        partition_start = 0
        for j in range(num_parsers_per_node):
            partition_end = partition_start + partition_size
            if j < time_slice_num_events % num_parsers_per_node:
                partition_end += 1
            parsed.append(parse_json._submit(
                args=[num_parser_out, partition_start, partition_end, generate_output],
                num_return_vals=num_parser_out,
                resources={node_resources[i] : 1},
                batch=BATCH))
            partition_start = partition_end
    if BATCH:
        parsed = ray.worker.global_worker.submit_batch(parsed)
    parsed = flatten(parsed)

    filtered, start_idx = [], 0
    for i in range(start_index, num_nodes):
        for _ in range(num_filters_per_node):
            filtered.append(filter._submit(
                args=[num_filter_out] + parsed[start_idx : start_idx + num_filter_in],
                num_return_vals=num_filter_out,
                resources={node_resources[i] : 1},
                batch=BATCH))
            start_idx += num_filter_in
    if BATCH:
        filtered = ray.worker.global_worker.submit_batch(filtered)
    filtered = flatten(filtered)

    shuffled, start_idx = [], 0
    num_return_vals = 1
    for i in range(start_index, num_nodes):
        for _ in range(num_projectors_per_node):
            batches = filtered[start_idx : start_idx + num_projector_in]
            shuffled.append(project_shuffle._submit(
                args=[num_projector_out, window_size] + batches,
                num_return_vals=num_return_vals,
                resources={node_resources[i] : 1},
                batch=BATCH))
            start_idx += num_projector_in
    if BATCH:
        shuffled = ray.worker.global_worker.submit_batch(shuffled)

    if BATCH:
        batch = [reducer.reduce.remote_batch(*shuffled) for reducer in reducers]
        ray.worker.global_worker.submit_batch(batch)
    else:
        [reducer.reduce.remote(*shuffled) for reducer in reducers]


def collect_redis_stats(redis_address, redis_port, campaign_ids=None):
    r = redis.StrictRedis(redis_address, port=redis_port)

    if campaign_ids is None:
        campaign_ids = []
        for key in r.keys():
            if b'seen_count' in r.hgetall(key):
                continue
            campaign_ids.append(key)

    latencies = []
    counts = defaultdict(int)
    for campaign_id in campaign_ids:
        windows = r.hgetall(campaign_id)
        for window, window_id in windows.items():
           seen, time_updated = r.hmget(window_id, [b'seen_count', b'time_updated'])
           seen = int(seen)
           time_updated = float(time_updated)
           window = float(window)
           counts[window] += seen
           latencies.append((window, time_updated - window))
    counts = sorted(counts.items(), key=lambda key: key[0])
    latencies = sorted(latencies, key=lambda key: key[0])
    print("Average latency:", np.mean([latency[1] for latency in latencies]))
    return latencies, counts

def write_stats(filename_prefix, redis_address, redis_port, campaign_ids=None, kill_time=None):
    latencies, throughputs = collect_redis_stats(redis_address, redis_port, campaign_ids)
    with open("{}-latency.out".format(filename_prefix), 'w+') as f:
        for window, latency in latencies:
            f.write("{},{}\n".format(window, latency))
        if kill_time is not None:
            f.write("KILL: {}".format(kill_time))
    with open("{}-throughput.out".format(filename_prefix), 'w+') as f:
        for window, throughput in throughputs:
            # The number of events is measured per window.
            throughput /= WINDOW_SIZE_SEC
            # We multiply by 3 because the filter step filters out 1/3 of the
            # events.
            throughput *= 3
            f.write("{},{}\n".format(window, throughput))


def compute_stats():
    i, total = 0, 0
    missed_total, miss_time_total = 0, 0
    counts = ray.get([reducer.count.remote() for reducer in reducers])
    for count in counts:
        print("Counted:", count)

    all_seen = ray.get([reducer.seen.remote() for reducer in reducers])
    for seen in all_seen:
        reducer_total = 0
        i += 1
        for key in seen:
            if key[1] < end_time:
                reducer_total += seen[key]
            else:
                missed_total += seen[key]
                miss_time_total += (end_time - key[1])
		#print("Missed by:", )
        total += reducer_total
        print("Seen by reducer", i, ": ", reducer_total)

    thput = 3 * total/exp_time
    miss_thput = 3 * missed_total/exp_time
    avg_miss_lat = miss_time_total / missed_total if missed_total != 0 else 0
    print("System Throughput: ", thput, "events/s")
    print("Missed: ", miss_thput, "events/s by", avg_miss_lat, "on average.")

def compute_checkpoint_overhead():
    checkpoints = ray.get([reducer.__ray_save__.remote() for reducer in reducers])
    start = time.time()
    pickles = [ray.cloudpickle.dumps(checkpoint) for checkpoint in checkpoints]
    end = time.time()
    print("Checkpoint pickle time", end - start)
    print("Checkpoint size(bytes):", [len(pickle) for pickle in pickles])

def get_node_names(num_nodes):
    node_names = set()
    while len(node_names) < num_nodes:
        hosts = [ping.remote() for _ in range(num_nodes * 100)]
        hosts, incomplete = ray.wait(hosts, timeout=30000) # timeout after 10s
        [node_names.add(ray.get(host_id)) for host_id in hosts]
        print(len(hosts), len(node_names))
        print("Nodes:", node_names)
        if incomplete:
            print("Timed-out after getting: ", len(hosts), "and missing", len(incomplete))
    return list(node_names)

def read_node_names(num_nodes):
    with open('/home/ubuntu/ray/benchmarks/stream/conf/priv-hosts-all') as f:
        lines = f.readlines()
    names = [l.strip() for l in lines][:num_nodes]
    if len(names) < num_nodes:
        raise IOError("File contains less than the requested number of nodes")
    return names

def ping_node(node_name):
    name = ray.get(ping._submit(args=[0.1], resources={node_name:1}))
    if name != node_name:
        print("Tried to ping", node_name, "but got", name)
    else:
        print("Pinged", node_name)
    return name

FIELDS = [
    u'user_id',
    u'page_id',
    u'ad_id',
    u'ad_type',
    u'event_type',
    u'event_time',
    u'ip_address',
    ]
INDICES = [
    (12, 48),
    (61, 97),
    (108, 144),
    (157, 165),
    (180, 190),
    (205, 221),
    (237, 244),
    ]
USER_ID    = 0
PAGE_ID    = 1
AD_ID      = 2
AD_TYPE    = 3
EVENT_TYPE = 4
EVENT_TIME = 5
IP_ADDRESS = 6
# The below array decoded is "view".
VIEW_EVENT_TYPE = np.array([ 34, 118, 105, 101, 119,  34,  32,  32,  32,  32], dtype=np.uint8)

########## helper functions #########

def generate_id():
    return str(uuid.uuid4()).encode('ascii')

# Take the ceiling of a timestamp to the end of the window.
def ts_to_window(timestamp, window_size):
    return ((float(timestamp) // window_size) + 1) * window_size

def flatten(x):
    if isinstance(x, Iterable):
        return [a for i in x for a in flatten(i)]
    else:
        return [x]

def init_reducer(node_index, node_resources, checkpoint, args=None):
    print("Starting reducer on node", node_index, "with args", args)
    if args is None:
        args = []

    if checkpoint:
        actor = ray.remote(
		num_cpus=0,
                checkpoint_interval=int(CHECKPOINT_INTERVAL / BATCH_SIZE_SEC),
                resources={ node_resources[node_index]: 1, })(Reducer).remote(*args)
    else:
        actor = ray.remote(
		num_cpus=0,
                resources={ node_resources[node_index]: 1, })(Reducer).remote(*args)
    ray.get(actor.clear.remote())
    # Take a checkpoint once every window.
    actor.node_index = node_index
    return actor

############### Tasks ###############

@ray.remote
def ping(time_to_sleep=0.01):
    time.sleep(time_to_sleep)
    return socket.gethostname()


def to_list(json_object):
    return [json_object[field] for field in FIELDS]

@ray.remote
def parse_json(num_ret_vals, partition_start, partition_end, *batches):
    """
    Parse batch of JSON events
    """
    parsed = np.array([to_list(ujson.loads(e.tobytes().decode('ascii'))) for
        batch in batches for e in batch[partition_start:partition_end]])
    return parsed if num_ret_vals == 1 else tuple(np.array_split(parsed, num_ret_vals))

@ray.remote
def filter_no_json(num_ret_vals, partition_start, partition_end, batch):
    """
    Parse batch of JSON events
    """
    batch = batch[partition_start:partition_end]
    return batch[np.all(
        batch[:, INDICES[EVENT_TYPE][0]:INDICES[EVENT_TYPE][1]] == VIEW_EVENT_TYPE,
        axis=1)]

def get_field(record, field):
    start, end = INDICES[field]
    return record[start:end].tobytes().decode('ascii')

def project_shuffle_no_json(num_reducers, window_size, *batches):
    shuffled = [defaultdict(int) for _ in range(num_reducers)]
    for batch in batches:
        window = ts_to_window(float(get_field(batch[0], EVENT_TIME)), window_size)
        for e in batch:
            ad_id = get_field(e, AD_ID).encode('ascii')
            cid = AD_TO_CAMPAIGN_MAP[ad_id]
            partition = CAMPAIGN_TO_PARTITION[cid]
            shuffled[partition][(cid, window)] += 1

    campaign_keys = [[] for _ in range(num_reducers)]
    window_keys = [[] for _ in range(num_reducers)]
    counts = [[] for _ in range(num_reducers)]
    max_partition_size = max(len(partition) for partition in shuffled)
    for i, partition in enumerate(shuffled):
        partition_size = 0
        for key, count in partition.items():
            campaign_keys[i].append(key[0])
            window_keys[i].append(key[1])
            counts[i].append(count)
            partition_size += 1
        while partition_size < max_partition_size:
            campaign_keys[i].append('')
            window_keys[i].append(0)
            counts[i].append(0)
            partition_size += 1
    return [np.array(campaign_keys), np.array(window_keys), np.array(counts)]

@ray.remote
def filter(num_ret_vals, *batches):
    """
    Filter events for view events
    """
    filtered_batches = []
    for batch in batches:
        filtered_batches.append(batch[batch[:, EVENT_TYPE] == u"view"])

    if len(batches) > 1:
        filtered = np.concatenate(filtered_batches)
    else:
        filtered = filtered_batches[0]
    return filtered if num_ret_vals == 1 else tuple(np.array_split(filtered, num_ret_vals))

def project_shuffle(num_reducers, window_size, *batches):
    """
    Project: e -> (campaign_id, window)
    Count by: (campaign_id, window)
    Shuffles by hash(campaign_id)
    """
    shuffled = [defaultdict(int) for _ in range(num_reducers)]
    for batch in batches:
        window = ts_to_window(batch[0][EVENT_TIME], window_size)
        for e in batch:
            cid = AD_TO_CAMPAIGN_MAP[e[AD_ID].encode('ascii')]
            partition = CAMPAIGN_TO_PARTITION[cid]
            shuffled[partition][(cid, window)] += 1

    campaign_keys = [[] for _ in range(num_reducers)]
    window_keys = [[] for _ in range(num_reducers)]
    counts = [[] for _ in range(num_reducers)]
    max_partition_size = max(len(partition) for partition in shuffled)
    for i, partition in enumerate(shuffled):
        partition_size = 0
        for key, count in partition.items():
            campaign_keys[i].append(key[0])
            window_keys[i].append(key[1])
            counts[i].append(count)
            partition_size += 1
        while partition_size < max_partition_size:
            campaign_keys[i].append('')
            window_keys[i].append(0)
            counts[i].append(0)
            partition_size += 1
    return [np.array(campaign_keys), np.array(window_keys), np.array(counts)]


class Reducer(object):

    def __init__(self, reduce_index, redis_address=None, redis_port=None):
        """
        Constructor
        """
        self.reduce_index = reduce_index
        # (campaign_id, window) --> count
        self.clear()

        if redis_address is not None:
            self.use_redis = True
            self.init_redis(redis_address, redis_port)

    def init_redis(self, redis_address, redis_port):
        print("Connecting to redis at {}:{}".format(redis_address, redis_port))
        self.redis_address = redis_address
        self.redis_port = redis_port
        self.redis = redis.StrictRedis(redis_address, port=redis_port)

    def __ray_save__(self):
        checkpoint = {
                "seen": list(self.seen.items()),
                "count": self.count,
                "reduce_index": self.reduce_index,
                }
        if self.use_redis:
            checkpoint["redis_address"] = self.redis_address
            checkpoint["redis_port"] = self.redis_port

        checkpoint = pickle.dumps(checkpoint)
        return checkpoint

    def __ray_restore__(self, checkpoint):
        checkpoint = pickle.loads(checkpoint)
        self.__init__(checkpoint["reduce_index"],
                      checkpoint.get("redis_address", None),
                      checkpoint.get("redis_port", None))
        for key, value in checkpoint["seen"]:
            self.seen[key] = value
        self.count = checkpoint["count"]

    def seen(self):
        return self.seen

    def count(self):
        return self.count

    def clear(self):
        """
        Clear all data structures.
        """
        self.seen = defaultdict(int)
        self.count = 0

    def get_or_create_window(self, campaign_id, window):
        window_id = self.redis.hget(campaign_id, window)
        if window_id is None:
            window_id = str(uuid.uuid4())
            self.redis.hset(campaign_id, window, window_id)
        return window_id

    def reduce(self, *partitions):
        """
        Reduce by key
        Increment and store in dictionary
        """
        try:
            min_window = min(window for _, window in self.seen)
        except ValueError:
            min_window = min(window for _, windows, _ in partitions for window in windows[self.reduce_index])

        keys_to_flush = []
        flush = False
        for campaign_ids, windows, counts in partitions:
            campaign_ids = campaign_ids[self.reduce_index]
            windows = windows[self.reduce_index]
            counts = counts[self.reduce_index]
            for campaign_id, window, count in zip(campaign_ids, windows, counts):
                if not campaign_id:
                    break
                key = (campaign_id, window)
                self.seen[key] += count
                self.count += count

                if window > min_window:
                    flush = True

        if flush:
            keys_to_flush = []
            if self.use_redis:
                pipe = self.redis.pipeline()
                for campaign_id, window in self.seen:
                    if window > min_window:
                        continue
                    keys_to_flush.append((campaign_id, window))
                    window_id = self.get_or_create_window(campaign_id, window)
                    pipe.hset(window_id, "seen_count", self.seen[(campaign_id, window)])
                    time_updated = time.time()
                    pipe.hset(window_id, "time_updated", time_updated)
                print("LATENCY:", time.time() - keys_to_flush[0][1])

                pipe.execute()
            else:
                for campaign_id, window in self.seen:
                    if window > min_window:
                        continue
                    keys_to_flush.append((campaign_id, window))
                    time_updated = time.time()
                    print("LATENCY:", window, time_updated - window)

            for key in keys_to_flush:
                del self.seen[key]


    def foo(self, *timestamps):
        """
        Reduce by key
        Increment and store in dictionary
        """
        timestamp = min([lst[0] for lst in timestamps])
        print("foo latency:", time.time() - timestamp - 0.05 * 5)


def restart_node(head_node_ip, node_ip, node_index):
    print("Restarting node", node_ip, "at", time.time())
    command = "ssh -i ~/devenv-key.pem {} bash -s < /home/ubuntu/ray/benchmark/cluster-scripts/restart_worker.sh {} {} 1 1 -1".format(node_ip, head_node_ip, node_index)
    with open(os.devnull, 'w') as fnull:
        subprocess.Popen(command.split(), stdout=fnull, stderr=fnull)

def kill_node(head_node_ip, node_ip, node_index):
    print("Killing node", node_ip, "at", time.time())
    command = "ssh -i ~/devenv-key.pem {} PATH=/home/ubuntu/anaconda3/bin/:$PATH ray stop".format(node_ip)
    with open(os.devnull, 'w') as fnull:
        subprocess.Popen(command.split(), stdout=fnull, stderr=fnull)

def start_node(head_node_ip, node_ip, node_index):
    print("Starting node", node_ip, "at", time.time())
    command = "ssh -i ~/devenv-key.pem {} bash -s < /home/ubuntu/ray/benchmark/cluster-scripts/start_worker.sh {} {} 128 1 1 -1 0".format(node_ip, head_node_ip, node_index)
    with open(os.devnull, 'w') as fnull:
        subprocess.Popen(command.split(), stdout=fnull, stderr=fnull)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump', type=str, default=None)
    parser.add_argument('--exp-time', type=int, default=60)
    parser.add_argument('--no-hugepages', action='store_true')
    parser.add_argument('--actor-checkpointing', action='store_true')
    parser.add_argument('--num-projectors', type=int, default=1)
    parser.add_argument('--num-filters', type=int, default=1)
    parser.add_argument('--num-generators', type=int, default=1)
    parser.add_argument('--num-nodes', type=int, required=True)
    parser.add_argument('--num-parsers', type=int, default=1)
    parser.add_argument('--num-reducers', type=int, default=1)
    parser.add_argument('--num-reducers-per-node', type=int, default=1)
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--target-throughput', type=int, default=1e5)
    parser.add_argument('--test-throughput', action='store_true')
    parser.add_argument('--time-slice-ms', type=int, default=100)
    parser.add_argument('--warmup-time', type=int, default=10)
    parser.add_argument('--reduce-redis-address', type=str, default=None)
    parser.add_argument('--output-filename', type=str, default="test")
    parser.add_argument('--use-json', action='store_true')
    parser.add_argument('--node-failure', type=int, default=-1)
    parser.add_argument('--window-size', type=float, default=10)
    args = parser.parse_args()

    checkpoint = args.actor_checkpointing
    exp_time = args.exp_time
    warmup_time = args.warmup_time
    num_nodes = args.num_nodes
    time_slice_num_events = int(args.target_throughput * BATCH_SIZE_SEC)

    num_generators_per_node = args.num_generators
    num_parsers_per_node = args.num_parsers 
    num_filters_per_node = args.num_filters
    num_projectors_per_node = args.num_projectors 

    num_generators = args.num_generators * num_nodes
    num_parsers = args.num_parsers * num_nodes
    num_filters = args.num_filters * num_nodes
    num_projectors = args.num_projectors * num_nodes
    num_reducers = args.num_reducers

    num_generator_out = max(1, num_parsers // num_generators)
    num_parser_in = max(1, num_generators // num_parsers)
    num_parser_out = max(1, num_filters // num_parsers)
    num_filter_in = max(1, num_parsers // num_filters)
    num_filter_out = max(1, num_projectors  // num_filters)
    num_projector_in = max(1, num_filters_per_node * num_filter_out // num_projectors_per_node)
    num_projector_out = num_reducers

    print("Per node setup: ")
    print("[", num_generators_per_node, "generators ] -->", num_generator_out)
    print(num_parser_in, "--> [", num_parsers_per_node, "parsers ] -->", num_parser_out  )
    print(num_filter_in, "--> [", num_filters_per_node, "filters ] -->", num_filter_out  )
    print(num_projector_in, "--> [", num_projectors_per_node, "projectors ] -->", num_projector_out)
    print(num_projectors, "--> [", num_reducers, "reducers]")

    node_resources = ["Node{}".format(i) for i in range(num_nodes)]

    if args.redis_address is None:
        huge_pages = not args.no_hugepages
        plasma_directory = "/mnt/hugepages" if huge_pages else None
        resources = [dict([(node_resource, num_generators + num_projectors + 
                            num_parsers + num_filters + num_reducers)]) 
                    for node_resource in node_resources]
        ray.worker._init(
            start_ray_local=True,
            redirect_output=True,
            use_raylet=True,
            resources=resources,
            num_local_schedulers=num_nodes,
            huge_pages=huge_pages,
            plasma_directory=plasma_directory)
    else:
        ray.init(redis_address="{}:6379".format(args.redis_address), use_raylet=True)
    time.sleep(2)

    partition = 0
    for campaign_id in CAMPAIGN_IDS:
        CAMPAIGN_TO_PARTITION[campaign_id] = partition
        partition += 1
        partition %= num_reducers
    project_shuffle = ray.remote(project_shuffle)
    project_shuffle_no_json = ray.remote(project_shuffle_no_json)

    print("Initializing generators...")
    print("Initializing reducers...")
    reducers = []
    assert num_reducers % args.num_reducers_per_node == 0
    num_reducer_nodes = num_reducers // args.num_reducers_per_node
    reduce_redis_address, reduce_redis_port = args.reduce_redis_address.split(':')
    for i in range(num_reducer_nodes):
        for j in range(args.num_reducers_per_node):
            reducers.append(init_reducer(i, node_resources, checkpoint,
                                         args=[i * args.num_reducers_per_node + j,
                                               reduce_redis_address,
                                               reduce_redis_port]))

    time.sleep(1)
    # Make sure the reducers are initialized.
    ray.get([reducer.clear.remote() for reducer in reducers])
    print("...finished initializing reducers:", len(reducers))

    print("Placing dependencies on nodes...")
    gen_dep = ray_warmup(reducers, node_resources, num_reducer_nodes)

    if exp_time > 0:
        if args.node_failure >= 0:
            workers = []
            with open('/home/ubuntu/ray/benchmark/cluster-scripts/workers.txt', 'r') as f:
                for line in f.readlines():
                    workers.append(line.strip())
            head_node_ip = workers.pop(0)
            node_to_kill = workers[args.node_failure]
            node_to_restart = workers[-1]
        try:
            time_to_sleep = BATCH_SIZE_SEC
            time.sleep(10) # TODO non-deterministic, fix

            print("Warming up...")
            start_time = time.time()
            end_time = start_time + warmup_time

            submit_tasks_fn = submit_tasks_no_json
            if args.use_json:
                submit_tasks_fn = submit_tasks

            time.sleep(10)
            for i in range(10):
                round_start = time.time()
                # generate (tstamp, [g,f,p,r])
                tstamp, batch_round = \
                    submit_tasks_fn(gen_dep, num_reducer_nodes, args.window_size, round_start)
                # submit g
                ray.worker.global_worker.submit_batch(batch_round[0])
                # submit [f,p,r]
                ray.worker.global_worker.submit_batch([item for sublist in batch_round[1:] for item in sublist])
                time.sleep(time_to_sleep)
                ray.wait([reducer.clear.remote() for reducer in reducers],
                         num_returns = len(reducers))
                print("finished round", i, "in", time.time() - round_start)

            time.sleep(10) # TODO non-deterministic, fix

            print("Measuring...")
            # Fail a node at a random time in a window 1/4 of the way through the experiment.
            failure_time = start_time + exp_time * 0.5 + WINDOW_SIZE_SEC * np.random.rand()
            killed = False

            # Make batch submission start time be a multiple of window size (10s)
            start_time = (time.time() // WINDOW_SIZE_SEC) * WINDOW_SIZE_SEC + WINDOW_SIZE_SEC
            # The first generate batch tstamp should be start time start_time
            # Subsequent generate tstamps = start_time + i* BATCH_SIZE_SEC
            end_time = start_time + exp_time
            next_round_time = start_time + BATCH_SIZE_SEC
            time.sleep(start_time - time.time())

            # call submit_tasks_fn (gen_dep, num_reducer_nodes, args.window_size, start_time,
            # for every round of batches
            # for every time stamp, call submit
            ### create a for loop that generates a vector of batch rounds by calling submit_tasks_fn in a loop
            # for the total expected number of rounds
            # aggregate batch rounds in the vector
            # all_batch_rounds = [ (t, [g,f,p,r]), (t, [g,f,p,r]), (t, [g,f,p,r]) ]
            all_batch_rounds = []
            for i in range(int(exp_time/BATCH_SIZE_SEC)):
                batch_round_tstamp, batch_round = \
                    submit_tasks_fn(gen_dep, num_reducer_nodes, args.window_size, start_time+i*BATCH_SIZE_SEC)
                all_batch_rounds.append((batch_round_tstamp, batch_round))

            # Go through all the generated batch rounds, submit generate batch to simulate
            # application activity for interval [ts, ts+BATCH_SIZE_SEC], sleep until
            # ts+BATCH_SIZE_SEC, submit the rest of the batch round

            # Start with submitting the initial generate batch
            # all_batch_rounds = [ (t, [g,f,p,r]), (t, [g,f,p,r]), (t, [g,f,p,r]) ]
            assert(len(all_batch_rounds) > 0)
            assert(len(all_batch_rounds[0][1]) > 0)
            # we expect all_batch_rounds[0][1][0] to be a list of generate tasks
            assert(len(all_batch_rounds[0][1][0]) > 0)
            ray.worker.global_worker.submit_batch(all_batch_rounds[0][1][0])
            for i in range(len(all_batch_rounds)):
                # get the next batch
                batch_round_tstamp, batch_round = all_batch_rounds[i]
                # The code below is triggering node failure if configured by experiment.
                if args.node_failure >= 0 and time.time() > failure_time and not killed:
                    #restart_node(head_node_ip, node_to_restart, args.node_failure)
                    kill_node(head_node_ip, node_to_kill, args.node_failure)
                    #start_node(head_node_ip, node_to_restart, args.node_failure)
                    killed = True

                # Calculate time to sleep and sleep until submitting the rest of round i
                time_to_sleep = batch_round_tstamp + BATCH_SIZE_SEC - time.time()
                if time_to_sleep > 0:
                    time.sleep(time_to_sleep)
                else:
                    print("WARNING: behind by", time_to_sleep * -1)

                # Submit the remaining task batches for this round of batches
                ray.worker.global_worker.submit_batch([item for sublist in batch_round[1:] for item in sublist])
                # Submit the generate batch from the next round of batches
                if i < len(all_batch_rounds) - 1:
                    tsnext, batch_round_next = all_batch_rounds[i+1]
                    next_generate_batch = batch_round_next[0] # generate batch is first in the batch round
                    ray.worker.global_worker.submit_batch(next_generate_batch)

            end_time = time.time()

            print("Finished in: ", (end_time - start_time), "s")

            print("Computing throughput...")
            if args.reduce_redis_address is not None:
                kill_time = None
                if args.node_failure >= 0:
                    kill_time = failure_time
                write_stats(args.output_filename, reduce_redis_address,
                            reduce_redis_port, campaign_ids=CAMPAIGN_IDS,
                            kill_time=kill_time)

            if args.dump is not None:
                print("Dumping...")
                ray.global_state.chrome_tracing_dump(filename=args.dump)

            #print("Computing checkpoint overhead...")
            #compute_checkpoint_overhead()

        except KeyboardInterrupt:
            print("Dumping current state...")
            ray.global_state.chrome_tracing_dump(filename=args.dump)
            exit()

