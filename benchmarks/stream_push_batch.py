import ray
import time
import uuid
from collections import defaultdict
import logging

import stream_push

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

@ray.remote
class Mapper(stream_push.ProcessingStream):
    def __init__(self, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.events = []

    def process_elements(self, elements):
        self.events += elements
        return elements

    def last(self):
        """ Helper method to compute the event throughput. """
        if len(self.events) > 0:
            timestamp = self.events[-1]
            return len(self.events), float(timestamp)
        else:
            return len(self.events), 0

@ray.remote
class Generator(stream_push.SourceStream):
    def __init__(self, time_slice_start_ms, time_slice_ms, time_slice_num_events, *downstream_nodes):
        super().__init__(None, *downstream_nodes)

        self.time_slice_start_ms = time_slice_start_ms
        self.time_slice_ms = time_slice_ms
        self.time_slice_num_events = time_slice_num_events

    def generate_elements(self):
        # Sleep until the start of the next time slice.
        self.time_slice_start_ms += self.time_slice_ms
        diff = (self.time_slice_start_ms / 1000) - time.time()
        if diff > 0:
            time.sleep(diff)
        elif diff < 0:
            log.warning("Falling behind by %f seconds", -1 * diff)

        # Generate the elements for this time slice.
        elements = []
        for _ in range(self.time_slice_num_events):
            now = time.time()
            elements.append(str(now))

        return elements


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--time-slice-ms', type=int, default=100)
    parser.add_argument('--target-throughput', type=int, default=1e5)

    parser.add_argument('--use-raylet', action='store_true')
    parser.add_argument('--num-mappers', type=int, required=False, default=1)
    parser.add_argument('--num-generators', type=int, default=1)

    args = parser.parse_args()

    ray.init(use_raylet=args.use_raylet)

    # The number of events to generate per time slice.
    time_slice_num_events = args.target_throughput / (1000 / args.time_slice_ms)
    time_slice_num_events /= args.num_generators
    time_slice_num_events = int(time_slice_num_events)

    sleep_time = 10

    mappers = [Mapper.remote() for _ in range(args.num_mappers)]
    ray.get([mapper.last.remote() for mapper in mappers])

    # Round up the starting time to the nearest time_slice_ms.
    time_slice_start_ms = (time.time() + 2) * 1000
    time_slice_start_ms = -(-time_slice_start_ms // args.time_slice_ms) * args.time_slice_ms

    generator_args = [time_slice_start_ms, args.time_slice_ms, time_slice_num_events]
    generators = stream_push.map_stream(args.num_generators, Generator, generator_args, mappers)
    time.sleep(1)

    # Start the generators.
    [generator.start.remote() for generator in generators]
    start = time.time()

    time.sleep(sleep_time)
    results = ray.get([mapper.last.remote() for mapper in mappers])
    for num_events, end in results:
        log.info("Achieved throughput for mapper was %d", num_events / (end - start))
