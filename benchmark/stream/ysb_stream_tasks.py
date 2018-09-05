# noqa all
from __future__ import print_function
from collections import defaultdict
from collections import Iterable
import numpy as np
import ray
import ray.cloudpickle as pickle
import socket
import time
import ujson
import uuid

from conf import *
from generate import *

FIELDS = [
    u'user_id',
    u'page_id',
    u'ad_id',
    u'ad_type',
    u'event_type',
    u'event_time',
    u'ip_address',
    ]
USER_ID    = 0
PAGE_ID    = 1
AD_ID      = 2
AD_TYPE    = 3
EVENT_TYPE = 4
EVENT_TIME = 5
IP_ADDRESS = 6

########## helper functions #########

def generate_id():
    return str(uuid.uuid4()).encode('ascii')

# Take the ceiling of a timestamp to the end of the window.
def ts_to_window(timestamp):
    return ((float(timestamp) // WINDOW_SIZE_SEC) + 1) * WINDOW_SIZE_SEC

def flatten(x):
    if isinstance(x, Iterable):
        return [a for i in x for a in flatten(i)]
    else:
        return [x]

def init_actor(node_index, node_resources, actor_cls, checkpoint, args=None):
    if args is None:
        args = []

    if checkpoint:
        actor = ray.remote(
		num_cpus=0,
                checkpoint_interval=int(WINDOW_SIZE_SEC / BATCH_SIZE_SEC),
                resources={ node_resources[node_index]: 1, })(actor_cls).remote(*args)
    else:
        actor = ray.remote(
		num_cpus=0,
                resources={ node_resources[node_index]: 1, })(actor_cls).remote(*args)
        # Take a checkpoint once every window.
    actor.node_index = node_index
    return actor

############### Tasks ###############

@ray.remote
def ping(time_to_sleep=0.01):
    time.sleep(time_to_sleep)
    return socket.gethostname()

@ray.remote
def warmup(*args):
    #x = np.ones(10 ** 8)
    # for _ in range(100):
    #     print(_)
    #     ray.put(x)
    for arg in args:
        pass


def to_list(json_object):
    return [json_object[field] for field in FIELDS]

@ray.remote
def parse_json(num_ret_vals, *batches):
    """
    Parse batch of JSON events
    """
    parsed = np.array([to_list(ujson.loads(e.tobytes().decode('ascii'))) for batch in batches for e in batch])
    return parsed if num_ret_vals == 1 else tuple(np.array_split(parsed, num_ret_vals))


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


@ray.remote
def project_shuffle(num_ret_vals, *batches):
    """
    Project: e -> (campaign_id, window)
    Count by: (campaign_id, window)
    Shuffles by hash(campaign_id)
    """
    shuffled = [defaultdict(int) for _ in range(num_ret_vals)]
    for batch in batches:
        for e in batch:
            window = ts_to_window(e[EVENT_TIME])
            cid = AD_TO_CAMPAIGN_MAP[e[AD_ID].encode('ascii')]
            shuffled[hash(cid) % num_ret_vals][(cid, window)] += 1
    return shuffled[0] if len(shuffled) == 1 else tuple(shuffled)


class Reducer(object):

    def __init__(self):
        """
        Constructor
        """
        # (campaign_id, window) --> count
        self.clear()

    def __ray_save__(self):
        checkpoint = pickle.dumps({
                "seen": list(self.seen.items()),
                "latencies": list(self.latencies.items()),
                "calls": self.calls,
                "count": self.count,
                })
        self.checkpoints.append(checkpoint)
        self.seen.clear()
        self.latencies.clear()
        return self.checkpoints

    def __ray_restore__(self, checkpoints):
        self.__init__()
        self.checkpoints = checkpoints
        if len(self.checkpoints) > 0:
            checkpoint = pickle.loads(self.checkpoints[-1])
            self.calls = checkpoint["calls"]
            self.count = checkpoint["count"]

    def seen(self):
        if not self.checkpoints:
            return self.seen
        else:
            seen = defaultdict(int)
            for checkpoint in self.checkpoints:
                checkpoint_seen = pickle.loads(checkpoint)["seen"]
                for key, count in checkpoint_seen:
                    seen[tuple(key)] += count
            return seen

    def get_latencies(self):
        if not self.checkpoints:
            return self.latencies
        else:
            latencies = defaultdict(int)
            for checkpoint in self.checkpoints:
                checkpoint_latencies = pickle.loads(checkpoint)["latencies"]
                for key, latency in checkpoint_latencies:
                    latencies[tuple(key)] = latency
            return latencies

    def count(self):
        return self.count

    def clear(self):
        """
        Clear all data structures.
        """
        self.seen = defaultdict(int)
        self.latencies = defaultdict(int)
        self.calls = 0
        self.count = 0
        self.checkpoints = []

    def reduce(self, *partitions):
        """
        Reduce by key
        Increment and store in dictionary
        """
        self.calls += 1
        printed = False
        for partition in partitions:
            for key in partition:
                self.seen[key] += partition[key]
                self.count += partition[key]
                latency = time.time() - key[1]
                if DEBUG and a == 0 and (self.calls % 10 == 0):
                    print(latency)
                    printed = True
                self.latencies[key] = max(self.latencies[key], latency)

