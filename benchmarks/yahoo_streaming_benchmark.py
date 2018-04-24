import ray
import time
import uuid
import logging
import simplejson as json
from collections import defaultdict
from collections import Counter

import stream_push

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

NUM_PAGE_IDS = 100
NUM_USER_IDS = 100
NUM_CAMPAIGNS = 10
NUM_ADS_PER_CAMPAIGN = 10
WINDOW_SIZE_SEC = 1

SLEEP_TIME = 10


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
        if len(self.events) > 0:
            log.setLevel(logging.INFO)
            log.info("Achieved throughput was %d", len(self.events) /
                     SLEEP_TIME)
            log.info("Latency: %f", sum(self.latencies.values()) /
                     len(self.latencies))


class ParseJson(stream_push.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.events = []

    def process_elements(self, elements):
        # json.loads appears to be faster on a single JSON list rather than a
        # list of JSON elements...
        return json.loads('[' + ', '.join(elements) + ']')


class Filter(stream_push.ProcessingStream):
    def __init__(self, debug_mode, *downstream_nodes):
        super().__init__(None, *downstream_nodes)
        self.debug_mode = debug_mode

    def process_elements(self, elements):
        if self.debug_mode:
            return elements
        else:
            return [element for element in elements if element["event_type"] ==
                    "view"]


class Project(stream_push.ProcessingStream):
    def __init__(self, ad_to_campaign_map, partition_func, *downstream_nodes):
        super().__init__(partition_func, *downstream_nodes)

        self.ad_to_campaign_map = ad_to_campaign_map

    def process_elements(self, elements):
        return [(self.ad_to_campaign_map[element["ad_id"]],
                 (int(element["event_time"] // WINDOW_SIZE_SEC) *
                  WINDOW_SIZE_SEC)) for element in elements]


class GroupBy(stream_push.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.windows = defaultdict(Counter)
        self.latencies = defaultdict(lambda: defaultdict(float))

    def process_elements(self, elements):
        for campaign_id, window in elements:
            self.windows[campaign_id][window] += 1
            new_latency_ms = (time.time() - window - WINDOW_SIZE_SEC) * 1000
            self.latencies[campaign_id][window] = max(
                self.latencies[campaign_id][window], new_latency_ms)

        return []


class EventGenerator(stream_push.SourceStream):
    def __init__(self, ad_to_campaign_map, time_slice_start_ms, time_slice_ms,
                 time_slice_num_events, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.ad_ids = list(ad_to_campaign_map.keys())

        self.user_ids = [str(uuid.uuid4()) for _ in range(NUM_USER_IDS)]
        self.page_ids = [str(uuid.uuid4()) for _ in range(NUM_PAGE_IDS)]
        self.event_types = ["view", "click", "purchase"]

        self.time_slice_start_ms = time_slice_start_ms
        self.time_slice_ms = time_slice_ms
        self.time_slice_num_events = time_slice_num_events

    def generate_elements(self):
        self.time_slice_start_ms += self.time_slice_ms

        # Sleep until the start of the next time slice.
        diff = (self.time_slice_start_ms / 1000) - time.time()
        if diff > self.time_slice_ms / 1000:
            time.sleep(diff)
        elif diff < -0.1:
            log.warning("Falling behind by %f seconds", -1 * diff)

        # Generate the JSON string of events for this time slice.
        events = []
        for i in range(self.time_slice_num_events):
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
            events.append(event)

        return events


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--time-slice-ms', type=int, default=100)
    parser.add_argument('--target-throughput', type=int, default=1e5)

    parser.add_argument('--use-raylet', action='store_true')
    parser.add_argument('--num-nodes', type=int, default=1)
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--no-hugepages', action='store_true')
    parser.add_argument('--test-throughput', action='store_true')
    parser.add_argument('--num-mappers', type=int, required=False, default=1)
    parser.add_argument('--num-generators', type=int, default=1)
    parser.add_argument('--num-reducers', type=int, default=1)
    parser.add_argument('--gcs-delay-ms', type=int)

    args = parser.parse_args()

    node_resources = ["Node{}".format(i) for i in range(args.num_nodes)]
    num_generators = args.num_generators * args.num_nodes
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
        return event[0]
    projectors = stream_push.group_by_stream(num_mappers, node_resources, Project,
                                             [ad_to_campaign_map], reducers,
                                             ad_id_key_func)
    filters = stream_push.map_stream(num_mappers, node_resources, Filter,
                                     [args.test_throughput], projectors)
    mappers = stream_push.map_stream(num_mappers, node_resources, ParseJson, [],
                                     filters)

    # Round up the starting time to the nearest time_slice_ms.
    time_slice_start_ms = (time.time() + 2) * 1000
    time_slice_start_ms = (-(-time_slice_start_ms // args.time_slice_ms) *
                           args.time_slice_ms)
    # Create the event generator source.
    generator_args = [ad_to_campaign_map, time_slice_start_ms,
                      args.time_slice_ms, time_slice_num_events]
    generators = stream_push.map_stream(num_generators, node_resources, EventGenerator,
                                        generator_args, mappers)

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

    if args.test_throughput:
        results = ray.get([reducer.last.remote() for reducer in reducers])
