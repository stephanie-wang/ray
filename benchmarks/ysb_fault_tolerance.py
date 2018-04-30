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

import stream_push_tasks

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

NUM_PAGE_IDS = 100
NUM_USER_IDS = 100
NUM_CAMPAIGNS = 10000
NUM_ADS_PER_CAMPAIGN = 10
WINDOW_SIZE_SEC = 1

SLEEP_TIME = 600

FIELDS = [
    "user_id",
    "page_id",
    "ad_id",
    "ad_type",
    "event_type",
    "event_time",
    "ip_address",
    ]
USER_ID    = 0
PAGE_ID    = 1
AD_ID      = 2
AD_TYPE    = 3
EVENT_TYPE = 4
EVENT_TIME = 5
IP_ADDRESS = 6


def warmup():
    x = np.ones(10 ** 8)
    for _ in range(100):
        ray.put(x)


class GroupBy(stream_push_tasks.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.windows = defaultdict(Counter)
        self.latencies = defaultdict(float)
        log.setLevel(logging.INFO)
        self.pid = os.getpid()

    def process_elements(self, batches):
        log.warn("groupby: %d at %f, event window is %f", self.pid, time.time(), batches[0][0][0][1])
        for elements in batches:
            for window, count in elements:
                self.windows[window[0]][window[1]] += count
                new_latency_ms = (time.time() - window[1] - WINDOW_SIZE_SEC) * 1000
                self.latencies[window] = max(
                    self.latencies[window], new_latency_ms)

        return []

    def last(self):
        """ Helper method to compute latencies. """
        return 0, self.latencies


@ray.remote
def parse_json(elements, start, end):
    log.warn("json: %d at %f", os.getpid(), time.time())
    # This could probably be made faster by writing the following in C++.
    # [{'user_id': elements[i][12:48].tobytes().decode('ascii'),
    #   'page_id': elements[i][61:97].tobytes().decode('ascii'),
    #   'ad_id': elements[i][108:144].tobytes().decode('ascii'),
    #   'ad_type': 'banner78',
    #   'event_type': elements[i][180:190].tobytes().decode('ascii'),
    #   'event_time': float(elements[i][204:220].tobytes().decode('ascii')),
    #   'ip_address': '1.2.3.4'} for i in range(elements.shape[0])]
    elements = [ujson.loads(x.tobytes().decode('ascii')) for x in elements[start:end]]
    log.warn("json: latency is %f", time.time() - float(elements[0][FIELDS[EVENT_TIME]]))
    return np.array([[element[field].encode('ascii') for field in FIELDS] for element in elements])

@ray.remote
def filter_json(*batches):
    log.warn("filter: %d at %f", os.getpid(), time.time())
    log.warn("filter: latency is %f, event timestamp is %f", time.time() - float(batches[0][0][EVENT_TIME]), float(batches[0][0][EVENT_TIME]))
    return np.concatenate([elements[elements[:, EVENT_TYPE] == b'view'] for elements in batches])

@ray.remote
def project_json(ad_to_campaign_map, num_reducers, *batches):
    start = time.time()
    log.warn("project: %d at %f", os.getpid(), time.time())
    log.warn("project: latency is %f, event timestamp is %f", start - float(batches[0][0][EVENT_TIME]), float(batches[0][0][EVENT_TIME]))
    window_partitions = [Counter() for _ in range(num_reducers)]

    for batch in batches:
        for element in batch:
            event_time = float(element[EVENT_TIME])
            window = (int(event_time // WINDOW_SIZE_SEC) *
                  WINDOW_SIZE_SEC)
            key = ad_to_campaign_map[element[AD_ID]]
            partition = sum(key) % num_reducers
            window_partitions[partition][(key, window)] += 1
    log.warn("project: took %f", time.time() - start)
    return [list(windows.items()) for windows in window_partitions]


class EventGenerator(stream_push_tasks.SourceStream):
    def __init__(self, ad_to_campaign_map_id, arrays, time_slice_ms,
                 time_slice_num_events, num_parsers, num_filters,
                 num_projectors, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.args = [ad_to_campaign_map_id, arrays, time_slice_ms,
                     time_slice_num_events, num_parsers, num_filters,
                     num_projectors, downstream_nodes]

        self.ad_to_campaign_map_id = ad_to_campaign_map_id[0]
        self.ad_ids_array, self.user_ids_array, self.page_ids_array, self.event_types_array = ray.get(arrays)

        self.time_slice_ms = float(time_slice_ms)
        self.time_slice_num_events = time_slice_num_events

        self.num_parsers = num_parsers
        self.num_filters = num_filters
        self.num_projectors = num_projectors


        self.throughput_at = 0
        self.num_elements = 0

        log.setLevel(logging.INFO)
        self.pid = os.getpid()

        # For speeding up JSON generation.
        id_array = np.empty(shape=(self.time_slice_num_events, 36), dtype=np.uint8)
        type_array = np.empty(shape=(self.time_slice_num_events, 10), dtype=np.uint8)
        time_array = np.empty(shape=(self.time_slice_num_events, 16), dtype=np.uint8)
        part1 = np.array(self.time_slice_num_events * [np.array(memoryview(b'{"user_id":"'), np.uint8)])
        part2 = np.array(self.time_slice_num_events * [np.array(memoryview(b'","page_id":"'), np.uint8)])
        part3 = np.array(self.time_slice_num_events * [np.array(memoryview(b'","ad_id":"'), np.uint8)])
        part4 = np.array(self.time_slice_num_events * [np.array(memoryview(b'","ad_type":"banner78","event_type":'), np.uint8)])
        part5 = np.array(self.time_slice_num_events * [np.array(memoryview(b',"event_time":"'), np.uint8)])
        part6 = np.array(self.time_slice_num_events * [np.array(memoryview(b'","ip_address":"1.2.3.4"}'), np.uint8)])
        self.indices = np.arange(self.time_slice_num_events)
        self.template = np.hstack([part1,
                                   id_array,
                                   part2,
                                   id_array,
                                   part3,
                                   id_array,
                                   part4,
                                   type_array,
                                   part5,
                                   time_array,
                                   part6])

        self.projected = []
        self.num_generate_tasks = 0

    def register_handle(self, self_handle):
        self.handle = self_handle

    def _push(self, elements, node_resource):
        put_latency = 0
        if len(self.downstream_actors) and len(elements):
            # Split the elements into equal-sized batches across all downstream
            # nodes.
            start = time.time()
            x = ray.put(elements)
            put_latency += (time.time() - start)

            print("Parse tasks")
            parsed = []
            batch_size = len(elements) // self.num_parsers
            start = 0
            for i in range(self.num_parsers):
                end = start + batch_size
                if i < (len(elements) % self.num_parsers):
                    end += 1
                parsed.append(parse_json._submit(args=[x, start, end], resources={node_resource: 1}))
                start = end

            print("Filter tasks")
            filtered = []
            batch_size = self.num_parsers // self.num_filters
            start = 0
            for i in range(self.num_filters):
                end = start + batch_size
                if i < (self.num_parsers % self.num_filters):
                    end += 1
                filtered.append(filter_json._submit(args=parsed[start:end], resources={node_resource: 1}))
                start = end

            print("Project tasks")
            batch_size = self.num_filters // self.num_projectors
            start = 0
            for i in range(self.num_projectors):
                end = start + batch_size
                if i < (self.num_filters % self.num_projectors):
                    end += 1
                args = [self.ad_to_campaign_map_id, len(self.downstream_actors)] + filtered[start:end]
                self.projected.append(project_json._submit(args=args, num_return_vals=len(self.downstream_actors), resources={node_resource: 1}))
                start = end

            print("Reduce tasks")
            #if time.time() % WINDOW_SIZE_SEC > 0.8:
            for i, reducer in enumerate(self.downstream_actors):
                reducer_batch = [batch[i] for batch in self.projected]
                reducer.push.remote(*reducer_batch)
            self.projected = []

        return put_latency

    def start_window(self, start_time, node_resource):
        self.start_time = start_time
        self.time_slice_ms = self.time_slice_ms
        self.num_generate_tasks = (float(WINDOW_SIZE_SEC * 1000) / self.time_slice_ms)
        self.generate(node_resource)

    def generate(self, node_resource):
        diff = self.start_time - time.time()
        if diff > 0:
            time.sleep(diff)
        elif diff < 0.1:
            log.warning("Falling behind by %f seconds", -1 * diff)

        log.warn("generate: %d at %f, current time is %f", self.pid, self.start_time, time.time())
        event_timestamp, elements = self.generate_elements(self.start_time)
        put_latency = self._push(elements, node_resource)

        after = time.time()
        self.num_elements += len(elements)
        if self.num_elements > 10000:
            log.warn("Throughput: %f per second", self.num_elements / (after - self.throughput_at))
            self.throughput_at = after
            self.num_elements = 0

        log.warn("%d finished at %f, put %f", self.pid, time.time(), put_latency)
        log.warn("generate: latency is %f", time.time() - self.start_time)

        self.num_generate_tasks -= 1
        if self.num_generate_tasks > 0:
            self.start_time += self.time_slice_ms / 1000
            self.handle.generate.remote(node_resource)

    def generate_elements(self, timestamp_float):
        self.template[:, 12:48] = self.user_ids_array[self.indices % NUM_USER_IDS]
        self.template[:, 61:97] = self.page_ids_array[self.indices % NUM_PAGE_IDS]
        self.template[:, 108:144] = self.ad_ids_array[self.indices % len(self.ad_ids_array)]
        self.template[:, 180:190] = self.event_types_array[self.indices % 3]
        timestamp = (str(timestamp_float)).encode('ascii')[:16]
        if len(timestamp) < 16:
            timestamp += b'0' * (16 - len(timestamp))
        self.template[:, 205:221] = np.tile(np.array(memoryview(timestamp), dtype=np.uint8),
                                            (self.time_slice_num_events, 1))

        return timestamp, self.template


def generate_ads():
    campaign_ids = [str(uuid.uuid4()).encode('ascii') for _ in range(NUM_CAMPAIGNS)]
    campaign_to_ad_map = {}
    ad_to_campaign_map = {}
    for campaign_id in campaign_ids:
        campaign_to_ad_map[campaign_id] = [str(uuid.uuid4()).encode('ascii') for _ in
                                           range(NUM_ADS_PER_CAMPAIGN)]
        for ad_id in campaign_to_ad_map[campaign_id]:
            ad_to_campaign_map[ad_id] = campaign_id
    return ad_to_campaign_map


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
    parser.add_argument('--num-projectors', type=int, required=False, default=1)
    parser.add_argument('--num-filters', type=int, required=False, default=1)
    parser.add_argument('--num-parsers', type=int, required=False, default=1)
    parser.add_argument('--num-generators', type=int, default=1)
    parser.add_argument('--num-reducers', type=int, default=1)
    parser.add_argument('--gcs-delay-ms', type=int)
    parser.add_argument('--test-time', type=int, required=True)

    args = parser.parse_args()

    node_resources = ["Node{}".format(i) for i in range(args.num_nodes)]
    worker_resources = node_resources[1:]
    num_workers = args.num_nodes - 1
    assert num_workers > 0
    num_generators = args.num_generators * num_workers
    num_parsers = args.num_parsers * num_workers
    num_filters = args.num_filters * num_workers
    num_projectors = args.num_projectors * num_workers
    num_reducers = args.num_reducers * num_workers

    huge_pages = not args.no_hugepages
    if huge_pages:
        plasma_directory = "/mnt/hugepages"
    else:
        plasma_directory = None

    if args.redis_address is None:
        num_tasks_per_node = args.num_generators + args.num_parsers + args.num_filters + args.num_projectors + args.num_reducers
        ray.worker._init(
                start_ray_local=True,
                redirect_output=True,
                use_raylet=args.use_raylet,
                num_local_schedulers=args.num_nodes,
                # Start each node with enough resources for all of the actors.
                resources=[
                    dict([(node_resource, num_tasks_per_node)]) for node_resource in
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

    #warmup_tasks = []
    #for node_resource in node_resources:
    #    warmup_tasks.append(ray.remote(resources={node_resource: 1})(warmup).remote())
    #ray.get(warmup_tasks)

    # The number of events to generate per time slice.
    time_slice_num_events = (args.target_throughput / (1000 /
                             args.time_slice_ms))
    time_slice_num_events /= num_generators
    time_slice_num_events = int(time_slice_num_events)

    # Generate the ad campaigns.
    ad_to_campaign_map = generate_ads()


    # Construct the streams.
    if args.test_throughput:
        reducers = [stream_push_tasks.init_actor(stream_push_tasks.get_node(i, len(worker_resources)), worker_resources, ThroughputLogger) for i in range(num_reducers)]
    else:
        reducers = [stream_push_tasks.init_actor(stream_push_tasks.get_node(i, len(worker_resources)), worker_resources, GroupBy) for i in range(num_reducers)]

    print("reducers", reducers)
    # Create the event generator source.
    ad_to_campaign_map_id = ray.put(ad_to_campaign_map)
    ad_ids_array = np.array([np.array(memoryview(x)) for x in list(ad_to_campaign_map.keys())])
    user_ids_array = np.array([np.array(memoryview(str(uuid.uuid4()).encode('ascii')), np.uint8) for _ in range(NUM_USER_IDS)])
    page_ids_array = np.array([np.array(memoryview(str(uuid.uuid4()).encode('ascii')), np.uint8) for _ in range(NUM_PAGE_IDS)])
    event_types_array = np.array([np.array(memoryview(b'"view"    ')),
                                  np.array(memoryview(b'"click"   ')),
                                  np.array(memoryview(b'"purchase"'))])
    arrays = [ray.put(ad_ids_array),
              ray.put(user_ids_array),
              ray.put(page_ids_array),
              ray.put(event_types_array),
              ]
    generator_args = [[ad_to_campaign_map_id],
                      arrays,
                      args.time_slice_ms, time_slice_num_events,
                      args.num_parsers, args.num_filters, args.num_projectors,
                      ]
    generator_args += reducers
    generators = []
    for node_index in range(len(worker_resources)):
        for _ in range(args.num_generators):
            generators.append(stream_push_tasks.init_actor(node_index, worker_resources, EventGenerator, args=generator_args))
    print("generators", generators)
    ray.get([generator.ready.remote() for generator in generators])
    ray.get([generator.register_handle.remote(generator) for generator in generators])

    time.sleep(1)

    # Ping the event generators every second to make sure they're still alive.
    start_time = time.time() // WINDOW_SIZE_SEC
    start_time += 2
    print("Starting at", start_time)
    for _ in range(args.test_time // 2):
        ray.get([generator.start_window.remote(start_time, worker_resources[generator.node_index]) for generator in generators])
        start_time += 1
        time_left = start_time - time.time()
        if time_left > 0:
            time.sleep(time_left)

    p = ray.services.all_processes[ray.services.PROCESS_TYPE_RAYLET][-1]
    p.kill()
    p.terminate()
    while p.poll() is None:
        time.sleep(0.1)

    # Ping the event generators every second to make sure they're still alive.
    # Schedule the second half of the experiment on the local node, to make
    # sure that the tasks that the event generator submits are scheduled
    # locally.
    for _ in range(args.test_time // 2):
        ray.get([generator.start_window.remote(start_time, node_resources[0]) for generator in generators])
        start_time += 1
        time_left = start_time - time.time()
        if time_left > 0:
            time.sleep(time_left)

    # Stop generating the events.
    #ray.get([generator.stop.remote() for generator in generators])

    all_latencies = []
    results = ray.get([reducer.last.remote() for reducer in reducers])
    for num_events, latencies in results:
        throughput = num_events / args.test_time
        total_latency = 0
        num_windows = 0
        for window, latency in latencies.items():
            if latency > 0:
                all_latencies.append((window[1], latency))
                total_latency += latency
                num_windows += 1
        if num_windows > 0:
            log.warn("latency: %d, throughput: %d", total_latency / num_windows, throughput)
    all_latencies.sort(key=lambda key: key[0])
    for window, latency in all_latencies:
        print(window, latency)
