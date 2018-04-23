import ray
import time
from collections import defaultdict
import logging
import hashlib

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class Stream(object):
    def __init__(self, *downstream_nodes, partition_func=None):
        self.downstream_nodes = downstream_nodes
        if partition_func is None:
            self.partition_func = lambda i, _: i % len(self.downstream_nodes)
        else:
            self.partition_func = lambda _, elm: partition_func(elm)

    def ready(self):
        return

    def _push(self, elements):
        put_latency = 0
        if len(self.downstream_nodes) and len(elements):
            partitions = {}
            for i in range(len(self.downstream_nodes)):
                partitions[i] = []

            # Split the elements into equal-sized batches across all downstream
            # nodes.
            for i, element in enumerate(elements):
                partition_index = self.partition_func(i, element)
                partitions[partition_index].append(element)
            for partition_index, partition in partitions.items():
                start = time.time()
                x = ray.put(partition)
                put_latency += (time.time() - start)
                self.downstream_nodes[partition_index].push.remote(x)
        return put_latency


class ProcessingStream(Stream):
    def push(self, elements):
        now = time.time()

        elements = self.process_elements(elements)
        put_latency = self._push(elements)

        latency = time.time() - now
        log.debug("latency: %s %f s put; %f s total", self.__class__.__name__,
                  put_latency, latency)

    def process_elements(self, elements):
        raise NotImplementedError()


class SourceStream(Stream):
    def start(self, self_handle):
        self.handle = self_handle
        self.generate()

    def stop(self):
        self.handle = None

    def generate(self):
        now = time.time()

        elements = self.generate_elements()
        put_latency = self._push(elements)

        latency = time.time() - now
        log.debug("latency: %s %f s put; %f s total",
                  self.__class__.__name__, put_latency, latency)

        if self.handle is not None:
            self.handle.generate.remote()

    def generate_elements(self, elements):
        raise NotImplementedError()


def map_stream(num_upstream_nodes, upstream_cls, args, downstream_nodes):
    """
    Create a set of nodes and connect the stream to a set of existing
    downstream nodes. Nodes are assigned round-robin, so that each upstream
    node is connected to an equal partition of the downstream nodes. If there
    are more upstream nodes than downstream nodes, than some nodes may share a
    partition.
    """
    # Assign downstream nodes to upstream nodes round-robin.
    downstream_node_assignment = defaultdict(list)
    if len(downstream_nodes) > 0:
        for i in range(max(num_upstream_nodes, len(downstream_nodes))):
            downstream_node_assignment[
                i % num_upstream_nodes
            ].append(downstream_nodes[i % len(downstream_nodes)])
    upstream_nodes = [upstream_cls.remote(
                      *args, *downstream_node_assignment[i]) for i in
                      range(num_upstream_nodes)]
    ray.get([node.ready.remote() for node in upstream_nodes])
    return upstream_nodes


def group_by_stream(num_upstream_nodes, upstream_cls, args, downstream_nodes,
                    partition_key_func):
    """
    Create a set of nodes and connect the stream to a set of existing
    downstream nodes. Each upstream node is connected to all downstream nodes.
    """
    num_partitions = len(downstream_nodes)
    args.append(lambda element: int(
        hashlib.md5(
            partition_key_func(element).encode("ascii")
        ).hexdigest(), 16) % num_partitions)

    upstream_nodes = [upstream_cls.remote(*args, *downstream_nodes) for _ in
                      range(num_upstream_nodes)]
    ray.get([node.ready.remote() for node in upstream_nodes])
    return upstream_nodes
