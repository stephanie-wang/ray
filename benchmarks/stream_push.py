import ray
import time
import uuid
from collections import defaultdict
import logging

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

class Stream(object):
    def __init__(self, partition_func, *downstream_nodes):
        self.downstream_nodes = downstream_nodes
        self.partition_func = partition_func
        if self.partition_func is None:
            self.partition_func = lambda i: i % len(self.downstream_nodes)

    def ready(self):
        return

    def _push(self, elements):
        if len(self.downstream_nodes) and len(elements):
            partitions = {}
            for i in range(len(self.downstream_nodes)):
                partitions[i] = []

            # Split the elements into equal-sized batches across all downstream
            # nodes.
            for i, element in enumerate(elements):
                partition_index = self.partition_func(i)
                partitions[partition_index].append(element)
            for partition_index, partition in partitions.items():
                start = time.time()
                x = ray.put(partition)
                log.debug("put: %f seconds", time.time() - start)
                self.downstream_nodes[partition_index].push.remote(x)
                log.debug("Took %f seconds", time.time() - start)

class ProcessingStream(Stream):
    def push(self, elements):
        elements = self.process_elements(elements)
        self._push(elements)

    def process_elements(self, elements):
        raise NotImplementedError()

class SourceStream(Stream):
    def start(self):
        while True:
            elements = self.generate_elements()
            self._push(elements)

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
            downstream_node_assignment[i % num_upstream_nodes].append(downstream_nodes[i % len(downstream_nodes)])

    upstream_nodes = [upstream_cls.remote(*args, *downstream_node_assignment[i]) for i in range(num_upstream_nodes)]
    ray.get([node.ready.remote() for node in upstream_nodes])
    return upstream_nodes

def group_by_stream(num_upstream_nodes, upstream_cls, args, downstream_nodes):
    """
    Create a set of nodes and connect the stream to a set of existing
    downstream nodes. Each upstream node is connected to all downstream nodes.
    """
    upstream_nodes = [upstream_cls.remote(*args, *downstream_nodes) for _ in range(num_upstream_nodes)]
    ray.get([node.ready.remote() for node in upstream_nodes])
    return upstream_nodes

