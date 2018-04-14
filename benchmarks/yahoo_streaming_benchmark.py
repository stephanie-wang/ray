import ray
import time
import uuid
import logging
import simplejson as json
import random
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


@ray.remote
class ThroughputLogger(stream_push.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(*downstream_nodes)

        self.events = []
        self.max_latency_ms = 0

    def process_elements(self, elements):
        self.events += elements
        for _, window in elements:
            latency_ms = (time.time() - window - WINDOW_SIZE_SEC) * 1000
            self.max_latency_ms = max(latency_ms, self.max_latency_ms)

    def last(self):
        """ Helper method to compute the event throughput. """
        if len(self.events) > 0:
            log.setLevel(logging.INFO)
            log.info("Achieved throughput was %d", len(self.events) /
                     SLEEP_TIME)
            log.info("Latency: %f", self.max_latency_ms)


@ray.remote
class ParseJson(stream_push.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(*downstream_nodes)

        self.events = []

    def process_elements(self, elements):
        return [json.loads(element) for element in elements]


@ray.remote
class Filter(stream_push.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(*downstream_nodes)

    def process_elements(self, elements):
        #return elements
        return [element for element in elements if element["event_type"] ==
                "view"]


@ray.remote
class Project(stream_push.ProcessingStream):
    def __init__(self, ad_to_campaign_map, partition_func, *downstream_nodes):
        super().__init__(*downstream_nodes)

        self.ad_to_campaign_map = ad_to_campaign_map

    def process_elements(self, elements):
        return [(self.ad_to_campaign_map[element["ad_id"]],
                 (int(element["event_time"] // WINDOW_SIZE_SEC) *
                  WINDOW_SIZE_SEC)) for element in elements]


@ray.remote
class GroupBy(stream_push.ProcessingStream):
    # TODO(swang): Shard the reducers.
    def __init__(self, *downstream_nodes):
        super().__init__(*downstream_nodes)

        self.windows = defaultdict(Counter)
        self.latencies = defaultdict(lambda: defaultdict(float))

    def process_elements(self, elements):
        for campaign_id, window in elements:
            self.windows[campaign_id][window] += 1
            new_latency_ms = (time.time() - window - WINDOW_SIZE_SEC) * 1000
            self.latencies[campaign_id][window] = max(
                self.latencies[campaign_id][window], new_latency_ms)

        return []


@ray.remote
class EventGenerator(stream_push.SourceStream):
    def __init__(self, ad_to_campaign_map, time_slice_start_ms, time_slice_ms,
                 time_slice_num_events, *downstream_nodes):
        super().__init__(*downstream_nodes)

        self.ad_ids = list(ad_to_campaign_map.keys())

        self.user_ids = [str(uuid.uuid4()) for _ in range(NUM_USER_IDS)]
        self.page_ids = [str(uuid.uuid4()) for _ in range(NUM_PAGE_IDS)]
        self.event_types = ["view", "click", "purchase"]

        self.time_slice_start_ms = time_slice_start_ms
        self.time_slice_ms = time_slice_ms
        self.time_slice_num_events = time_slice_num_events

    def generate_elements(self):
        # Sleep until the start of the next time slice.
        self.time_slice_start_ms += self.time_slice_ms
        diff = (self.time_slice_start_ms / 1000) - time.time()
        if diff > (0.2 * self.time_slice_ms / 1000):
            time.sleep(diff)
        elif diff < -0.1:
            log.warning("Falling behind by %f seconds", -1 * diff)

        # Generate the elements for this time slice.
        elements = []
        for i in range(self.time_slice_num_events):
            timestamp = (self.time_slice_start_ms + random.randint(0,
                         self.time_slice_ms - 1)) / 1000
            event = '{{' \
                    '"user_id": "{}",' \
                    '"page_id": "{}",' \
                    '"ad_id": "{}",' \
                    '"ad_type": "banner78",' \
                    '"event_type": "{}",' \
                    '"event_time": {},' \
                    '"ip_address": "1.2.3.4",' \
                    '"generation_time": {}' \
                    '}}'.format(
                        self.user_ids[i % len(self.user_ids)],
                        self.page_ids[i % len(self.page_ids)],
                        self.ad_ids[i % len(self.ad_ids)],
                        self.event_types[i % len(self.event_types)],
                        timestamp,
                        time.time())
            elements.append(event)

        return elements


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--time-slice-ms', type=int, default=100)
    parser.add_argument('--target-throughput', type=int, default=1e5)

    parser.add_argument('--use-raylet', action='store_true')
    parser.add_argument('--test-throughput', action='store_true')
    parser.add_argument('--num-mappers', type=int, required=False, default=1)
    parser.add_argument('--num-generators', type=int, default=1)
    parser.add_argument('--num-reducers', type=int, default=1)

    args = parser.parse_args()

    ray.init(use_raylet=args.use_raylet)

    # The number of events to generate per time slice.
    time_slice_num_events = (args.target_throughput / (1000 /
                             args.time_slice_ms))
    time_slice_num_events /= args.num_generators
    time_slice_num_events = int(time_slice_num_events)
    # Round up the starting time to the nearest time_slice_ms.
    time_slice_start_ms = (time.time() + 2) * 1000
    time_slice_start_ms = (-(-time_slice_start_ms // args.time_slice_ms) *
                           args.time_slice_ms)

    # Generate the ad campaigns.
    campaign_ids = [str(uuid.uuid4()) for _ in range(NUM_CAMPAIGNS)]
    campaign_to_ad_map = {}
    ad_to_campaign_map = {}
    for campaign_id in campaign_ids:
        campaign_to_ad_map[campaign_id] = [str(uuid.uuid4()) for _ in
                                           range(NUM_ADS_PER_CAMPAIGN)]
        for ad_id in campaign_to_ad_map[campaign_id]:
            ad_to_campaign_map[ad_id] = campaign_id

    # Construct the streams.
    if args.test_throughput:
        reducers = [ThroughputLogger.remote() for _ in
                    range(args.num_reducers)]
    else:
        reducers = [GroupBy.remote() for _ in range(args.num_reducers)]

    def ad_id_key_func(event):
        return event["ad_id"]
    projectors = stream_push.group_by_stream(args.num_mappers, Project,
                                             [ad_to_campaign_map], reducers,
                                             ad_id_key_func)
    filters = stream_push.map_stream(args.num_mappers, Filter, [], projectors)
    mappers = stream_push.map_stream(args.num_mappers, ParseJson, [],
                                     filters)

    # Create the event generator source.
    ray.get([mapper.ready.remote() for mapper in mappers])
    generator_args = [ad_to_campaign_map, time_slice_start_ms,
                      args.time_slice_ms, time_slice_num_events]
    generators = stream_push.map_stream(args.num_generators, EventGenerator,
                                        generator_args, mappers)
    time.sleep(1)

    # Start the event generators.
    [generator.start.remote() for generator in generators]
    start = time.time()

    time.sleep(SLEEP_TIME)
    if args.test_throughput:
        results = ray.get([reducer.last.remote() for reducer in reducers])
