import ray
import time
from collections import defaultdict
import logging
import hashlib
import os

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class Stream(object):
    def __init__(self, partition_func, *downstream_actors):
        self.downstream_actors = downstream_actors
        if partition_func is not None:
            self.partition_func = lambda _, elm: partition_func(elm)
        else:
            self.partition_func = None

    def ready(self):
        return

    def _push(self, elements):
        put_latency = 0
        if len(self.downstream_actors) and len(elements):
            partitions = {}
            # Split the elements into equal-sized batches across all downstream
            # nodes.
            if self.partition_func is None:
                start = time.time()
                x = ray.put(elements)
                put_latency += (time.time() - start)

                batch_size = len(elements) // len(self.downstream_actors)
                start = 0
                for i, downstream_actor in enumerate(self.downstream_actors):
                    end = start + batch_size
                    if i < (len(elements) % len(self.downstream_actors)):
                        end += 1
                    downstream_actor.push.remote(start, end, x)
                    start = end

            else:
                for i in range(len(self.downstream_actors)):
                    partitions[i] = []
                for i, element in enumerate(elements):
                    partition_index = self.partition_func(i, element)
                    partitions[partition_index].append(element)

                for partition_index, partition in partitions.items():
                    start = time.time()
                    x = ray.put(partition)
                    put_latency += (time.time() - start)
                    self.downstream_actors[partition_index].push.remote(0, len(partition), x)
        return put_latency


class ProcessingStream(Stream):
    def push(self, *elements):
        now = time.time()

        elements = self.process_elements(elements)
        put_latency = self._push(elements)

        latency = time.time() - now
        log.debug("latency: %s %f s put; %f s total", self.__class__.__name__,
                  put_latency, latency)
        log.info("%d finished at %f, put %f", os.getpid(), time.time(), put_latency)

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


def get_node(actor_index, num_nodes):
    return actor_index % num_nodes

def actors_on_node(actors, node_index, num_nodes):
    return [actor for i, actor in enumerate(actors) if get_node(i, num_nodes) == node_index]


def init_actor(node_index, node_resources, actor_cls, args=None, checkpoint_interval=-1):
    if args is None:
        args = []
    actor = ray.remote(resources={
        node_resources[node_index]: 1,
        },
        checkpoint_interval=checkpoint_interval)(actor_cls).remote(*args)
    actor.node_index = node_index
    return actor


def map_stream(num_upstream_actors, node_resources, upstream_cls, args, downstream_actors):
    """
    Create a set of nodes and connect the stream to a set of existing
    downstream nodes. Nodes are assigned round-robin, so that each upstream
    node is connected to an equal partition of the downstream nodes. If there
    are more upstream nodes than downstream nodes, than some nodes may share a
    partition.
    """
    # Assign downstream nodes to upstream nodes round-robin.
    downstream_assignment = defaultdict(list)
    if len(downstream_actors) > 0:
        for i in range(max(num_upstream_actors, len(downstream_actors))):
            downstream_assignment[
                i % num_upstream_actors
            ].append(downstream_actors[i % len(downstream_actors)])
    upstream_actors = []
    for i in range(num_upstream_actors):
        downstream_actors = downstream_assignment[i]
        node_index = downstream_actors[0].node_index
        upstream_args = args + downstream_actors
        actor = init_actor(node_index, node_resources, upstream_cls, upstream_args)
        upstream_actors.append(actor)
    ray.get([node.ready.remote() for node in upstream_actors])
    return upstream_actors


def group_by_stream(num_upstream_actors, node_resources, upstream_cls, args, downstream_actors,
                    partition_key_func):
    """
    Create a set of nodes and connect the stream to a set of existing
    downstream nodes. Each upstream node is connected to all downstream nodes.
    """
    num_partitions = len(downstream_actors)
    args.append(lambda element: partition_key_func(element) % num_partitions)
    args += downstream_actors

    upstream_actors = []
    for i in range(num_upstream_actors):
        node = get_node(i, len(node_resources))
        actor = init_actor(node, node_resources, upstream_cls, args)
        upstream_actors.append(actor)
    ray.get([node.ready.remote() for node in upstream_actors])
    return upstream_actors
