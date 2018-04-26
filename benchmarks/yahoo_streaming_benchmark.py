import ray
import os
import time
import uuid
import logging
import simplejson as json
from collections import defaultdict
from collections import Counter
import numpy as np
import ujson

import stream_push

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

NUM_PAGE_IDS = 100
NUM_USER_IDS = 100
NUM_CAMPAIGNS = 10000
NUM_ADS_PER_CAMPAIGN = 10
WINDOW_SIZE_SEC = 1

SLEEP_TIME = 600


def warmup():
    x = np.ones(10 ** 8)
    for _ in range(100):
        ray.put(x)


class ThroughputLogger(stream_push.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.events = []
        self.latencies = defaultdict(float)

    def process_elements(self, elements):
        self.events += elements
        for campaign_id, window in elements:
            latency_ms = (time.time() - window - WINDOW_SIZE_SEC) * 1000
            if latency_ms > self.latencies[(campaign_id, window)]:
                self.latencies[(campaign_id, window)] = latency_ms

    def last(self):
        """ Helper method to compute the event throughput. """
        latency = -1
        throughput = 0
        if len(self.events) > 0:
            latency = sum(self.latencies.values()) / len(self.latencies)
            throughput = len(self.events) / SLEEP_TIME
            log.setLevel(logging.INFO)
            log.info("Achieved throughput was %d", throughput)
            log.info("Latency: %f", latency)
        return len(self.events), self.latencies


class ParseJson(stream_push.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.events = []
        log.setLevel(logging.INFO)
        self.pid = os.getpid()

    def process_elements(self, elements):
        log.info("json: %d at %f", self.pid, time.time())
        # json.loads appears to be faster on a single JSON list rather than a
        # list of JSON elements...
        return json.loads('[' + ', '.join(elements) + ']')

    def process_elements2(self, elements):
        log.info("json: %d at %f", self.pid, time.time())
        # This could probably be made faster by writing the following in C++.
        # [{'user_id': elements[i][12:48].tobytes().decode('ascii'),
        #   'page_id': elements[i][61:97].tobytes().decode('ascii'),
        #   'ad_id': elements[i][108:144].tobytes().decode('ascii'),
        #   'ad_type': 'banner78',
        #   'event_type': elements[i][180:190].tobytes().decode('ascii'),
        #   'event_time': float(elements[i][204:220].tobytes().decode('ascii')),
        #   'ip_address': '1.2.3.4'} for i in range(elements.shape[0])]
        return [ujson.loads(x.tobytes().decode('ascii')) for x in elements]


class Filter(stream_push.ProcessingStream):
    def __init__(self, debug_mode, *downstream_nodes):
        super().__init__(None, *downstream_nodes)
        self.debug_mode = debug_mode
        log.setLevel(logging.INFO)
        self.pid = os.getpid()

    def process_elements(self, elements):
        log.info("filter: %d at %f", self.pid, time.time())
        return [element for element in elements if element["event_type"] ==
                "view"]


class Project(stream_push.ProcessingStream):
    def __init__(self, ad_to_campaign_map, partition_func, *downstream_nodes):
        super().__init__(partition_func, *downstream_nodes)

        self.ad_to_campaign_map = ad_to_campaign_map
        log.setLevel(logging.INFO)
        self.pid = os.getpid()

        self.windows = Counter()
        self.earliest_time = None

    def process_elements(self, elements):
        log.info("project: %d at %f", self.pid, time.time())
        emit = False
        for element in elements:
            event_time = element["event_time"]
            window = (int(event_time // WINDOW_SIZE_SEC) *
                  WINDOW_SIZE_SEC)
            if self.earliest_time is None or event_time < self.earliest_time:
                self.earliest_time = event_time
            if event_time % WINDOW_SIZE_SEC > 0.8:
                emit = True
            self.windows[(self.ad_to_campaign_map[element["ad_id"]], window)] += 1

        if emit or (time.time() - self.earliest_time) > 0.5:
            elements = list(self.windows.items())
            self.windows.clear()
            self.earliest_time = None
            return elements
        else:
            return []

class GroupBy(stream_push.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.windows = defaultdict(Counter)
        self.latencies = defaultdict(float)
        log.setLevel(logging.INFO)
        self.pid = os.getpid()

    def process_elements(self, elements):
        log.info("groupby: %d at %f", self.pid, time.time())
        for window, count in elements:
            self.windows[window[0]][window[1]] += count
            new_latency_ms = (time.time() - window[1] - WINDOW_SIZE_SEC) * 1000
            self.latencies[window] = max(
                self.latencies[window], new_latency_ms)

        return []

    def last(self):
        """ Helper method to compute latencies. """
        return 0, self.latencies


class EventGenerator(stream_push.SourceStream):
    def __init__(self, ad_to_campaign_map, time_slice_start_ms, time_slice_ms,
                 time_slice_num_events, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.ad_ids = list(ad_to_campaign_map.keys())

        self.user_ids = [str(uuid.uuid4()) for _ in range(NUM_USER_IDS)]
        self.page_ids = [str(uuid.uuid4()) for _ in range(NUM_PAGE_IDS)]
        self.event_types = ["view", "click", "purchase"]

        self.time_slice_start_ms = time_slice_start_ms
        self.time_slice_ms = float(time_slice_ms)
        self.time_slice_num_events = time_slice_num_events

        self.throughput_at = 0
        self.num_elements = 0

        log.setLevel(logging.INFO)
        self.pid = os.getpid()

        # For speeding up JSON generation.
        id_array = np.empty(shape=(self.time_slice_num_events, 36), dtype=np.uint8)
        type_array = np.empty(shape=(self.time_slice_num_events, 10), dtype=np.uint8)
        time_array = np.empty(shape=(self.time_slice_num_events, 16), dtype=np.uint8)
        self.part1 = np.array(self.time_slice_num_events * [np.array(memoryview(b'{"user_id":"'), np.uint8)])
        self.part2 = np.array(self.time_slice_num_events * [np.array(memoryview(b'","page_id":"'), np.uint8)])
        self.part3 = np.array(self.time_slice_num_events * [np.array(memoryview(b'","ad_id":"'), np.uint8)])
        self.part4 = np.array(self.time_slice_num_events * [np.array(memoryview(b'","ad_type":"banner78","event_type":'), np.uint8)])
        self.part5 = np.array(self.time_slice_num_events * [np.array(memoryview(b',"event_time":'), np.uint8)])
        self.part6 = np.array(self.time_slice_num_events * [np.array(memoryview(b',"ip_address":"1.2.3.4"}'), np.uint8)])
        self.indices = np.arange(self.time_slice_num_events)
        self.template = np.hstack([self.part1,
                                   id_array,
                                   self.part2,
                                   id_array,
                                   self.part3,
                                   id_array,
                                   self.part4,
                                   type_array,
                                   self.part5,
                                   time_array,
                                   self.part6])

    def generate(self):
        now = time.time()

        log.info("generate: %d at %f", self.pid, time.time())
        elements = self.generate_elements()
        put_latency = self._push(elements)

        after = time.time()
        self.num_elements += len(elements)
        if self.num_elements > 10000:
            log.info("Throughput: %f per second", self.num_elements / (after - self.throughput_at))
            self.throughput_at = after
            self.num_elements = 0

        latency = time.time() - now
        latency -= self.time_slice_ms / 1000
        if latency < 0:
            time.sleep(-1 * latency)
        elif latency > 0.1:
            log.warning("Falling behind by %f seconds", latency)
        log.info("%d finished at %f, put %f", self.pid, time.time(), put_latency)

        log.debug("latency: %s %f s put; %f s total",
                  self.__class__.__name__, put_latency, latency)

        if self.handle is not None:
            self.handle.generate.remote()

    def generate_elements(self):
        #self.time_slice_start_ms += self.time_slice_ms

        ## Sleep until the start of the next time slice.
        #diff = (self.time_slice_start_ms / 1000) - time.time()
        #if diff > self.time_slice_ms / 1000:
        #    time.sleep(diff)
        #elif diff < -0.1:
        #    log.warning("Falling behind by %f seconds", -1 * diff)

        # Generate the JSON string of events for this time slice.
        #events = []
        #for i in range(self.time_slice_num_events):
        #    event = (
        #        '{'
        #        '"user_id": "' + self.user_ids[i % len(self.user_ids)] + '",'
        #        '"page_id": "' + self.page_ids[i % len(self.page_ids)] + '",'
        #        '"ad_id": "' + self.ad_ids[i % len(self.ad_ids)] + '",'
        #        '"ad_type": "banner78",'
        #        '"event_type": "' + self.event_types[i % len(self.event_types)] + '",'
        #        '"event_time": ' + str(time.time()) + ','
        #        '"ip_address": "1.2.3.4"'
        #        '}')
        #    events.append(event)
        i = np.random.randint(1000000)
        event = (
                '{'
                '"user_id": "' + self.user_ids[i % len(self.user_ids)] + '",'
                '"page_id": "' + self.page_ids[i % len(self.page_ids)] + '",'
                '"ad_id": "' + self.ad_ids[i % len(self.ad_ids)] + '",'
                '"ad_type": "banner78",'
                '"event_type": "' + self.event_types[i % len(self.event_types)] + '",'
                '"event_time": ' + str(time.time()) + ','
                '"ip_address": "1.2.3.4"'
                '}')
        events = [event for _ in range(self.time_slice_num_events)]

        return events

    def generate_elements2(self):
        self.template[:, 12:48] = self.user_ids_array[self.indices % NUM_USER_IDS]
        self.template[:, 61:97] = self.page_ids_array[self.indices % NUM_PAGE_IDS]
        self.template[:, 108:144] = self.ad_ids_array[self.indices % len(self.ad_ids)]
        self.template[:, 180:190] = self.event_types_array[self.indices % 3]
        self.template[:, 204:220] = np.tile(np.array(memoryview((str(time.time())).encode('ascii')[:16]), dtype=np.uint8),
                                            (self.time_slice_num_events, 1))

        return self.template


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--time-slice-ms', type=int, default=100)
    parser.add_argument('--target-throughput', type=int, default=1e5)

    parser.add_argument('--use-raylet', action='store_true')
    parser.add_argument('--num-nodes', type=int, required=True)
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--no-hugepages', action='store_true')
    parser.add_argument('--test-throughput', action='store_true')
    parser.add_argument('--num-mappers', type=int, required=False, default=1)
    parser.add_argument('--num-parsers', type=int, required=False, default=1)
    parser.add_argument('--num-generators', type=int, default=1)
    parser.add_argument('--num-reducers', type=int, default=1)
    parser.add_argument('--gcs-delay-ms', type=int)

    args = parser.parse_args()

    node_resources = ["Node{}".format(i) for i in range(1, args.num_nodes)]
    args.num_nodes -= 1
    num_generators = args.num_generators * args.num_nodes
    num_parsers = args.num_parsers * args.num_nodes
    num_mappers = args.num_mappers * args.num_nodes
    num_reducers = args.num_reducers * args.num_nodes

    huge_pages = not args.no_hugepages
    if huge_pages:
        plasma_directory = "/mnt/hugepages"
    else:
        plasma_directory = None

    if args.redis_address is None:
        ray.worker._init(
                start_ray_local=True,
                redirect_output=True,
                use_raylet=args.use_raylet,
                num_local_schedulers=args.num_nodes,
                # Start each node with enough resources for all of the actors.
                resources=[
                    dict([(node_resource, num_generators + num_mappers * 3 +
                           num_reducers)]) for node_resource in
                    node_resources
                    ],
                gcs_delay_ms=args.gcs_delay_ms if args.gcs_delay_ms is not None else -1,
                huge_pages=huge_pages,
                plasma_directory=plasma_directory,
                use_task_shard=True
                )
    else:
        ray.init(
                redis_address=args.redis_address,
                use_raylet=args.use_raylet)
    time.sleep(3)

    warmup_tasks = []
    for node_resource in node_resources:
        warmup_tasks.append(ray.remote(resources={node_resource: 1})(warmup).remote())
    ray.get(warmup_tasks)

    # The number of events to generate per time slice.
    time_slice_num_events = (args.target_throughput / (1000 /
                             args.time_slice_ms))
    time_slice_num_events /= num_generators
    time_slice_num_events = int(time_slice_num_events)

    # Generate the ad campaigns.
    campaign_ids = [str(uuid.uuid4()) for _ in range(NUM_CAMPAIGNS)]
    campaign_to_ad_map = {}
    ad_to_campaign_map = {}
    for campaign_id in campaign_ids:
        campaign_to_ad_map[campaign_id] = [str(uuid.uuid4()) for _ in
                                           range(NUM_ADS_PER_CAMPAIGN)]
        for ad_id in campaign_to_ad_map[campaign_id]:
            ad_to_campaign_map[ad_id] = campaign_id

    actor_placement = {}

    # Construct the streams.
    actor_placement = defaultdict(list)
    if args.test_throughput:
        reducers = [stream_push.init_actor(stream_push.get_node(i, len(node_resources)), node_resources, ThroughputLogger) for i in range(num_reducers)]
    else:
        reducers = [stream_push.init_actor(stream_push.get_node(i, len(node_resources)), node_resources, GroupBy) for i in range(num_reducers)]

    def ad_id_key_func(event):
        campaign_id = event[0][0]
        campaign_id = campaign_id.encode('ascii')
        return sum(campaign_id)
    print(len(reducers), "reducers")
    projectors = stream_push.group_by_stream(num_mappers, node_resources, Project,
                                             [ad_to_campaign_map], reducers,
                                             ad_id_key_func)
    filters = stream_push.map_stream(num_mappers, node_resources, Filter,
                                     [args.test_throughput], projectors)
    mappers = stream_push.map_stream(num_parsers, node_resources, ParseJson, [],
                                     filters)

    # Round up the starting time to the nearest time_slice_ms.
    time_slice_start_ms = (time.time() + 10) * 1000
    time_slice_start_ms = (-(-time_slice_start_ms // args.time_slice_ms) *
                           args.time_slice_ms)
    # Create the event generator source.
    generator_args = [ad_to_campaign_map, time_slice_start_ms,
                      args.time_slice_ms, time_slice_num_events]
    generators = stream_push.map_stream(num_generators, node_resources, EventGenerator,
                                        generator_args, mappers)
    print(len(generators), "generators")

    ray.get([reducer.ready.remote() for reducer in reducers])
    ray.get([projector.ready.remote() for projector in projectors])
    ray.get([f.ready.remote() for f in filters])
    ray.get([mapper.ready.remote() for mapper in mappers])
    ray.get([generator.ready.remote() for generator in generators])

    time.sleep(1)

    # Start the event generators.
    [generator.start.remote(generator) for generator in generators]
    start = time.time()

    # Ping the event generators every second to make sure they're still alive.
    for _ in range(SLEEP_TIME):
        ray.get([generator.ready.remote() for generator in generators])
        time.sleep(1)

    # Stop generating the events.
    ray.get([generator.stop.remote() for generator in generators])

    all_latencies = []
    results = ray.get([reducer.last.remote() for reducer in reducers])
    for num_events, latencies in results:
        throughput = num_events / SLEEP_TIME
        total_latency = 0
        num_windows = 0
        for window, latency in latencies.items():
            if latency > 0:
                all_latencies.append((window[1], latency))
                total_latency += latency
                num_windows += 1
        log.info("latency: %d, throughput: %d", total_latency / num_windows, throughput)
    all_latencies.sort(key=lambda key: key[0])
    for window, latency in all_latencies:
        print(window, latency)
