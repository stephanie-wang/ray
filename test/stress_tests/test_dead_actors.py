#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import sys
import time

import ray

logger = logging.getLogger(__name__)

ray.init(redis_address="localhost:6379")


@ray.remote
class Child(object):
    def __init__(self, death_probability):
        self.death_probability = death_probability

    def ping(self):
        # Exit process with some probability.
        exit_chance = np.random.rand()
        if exit_chance > self.death_probability:
            sys.exit(-1)


@ray.remote
class Parent(object):
    def __init__(self, num_children, death_probability):
        self.death_probability = death_probability
        self.children = [
            Child.remote(death_probability) for _ in range(num_children)
        ]

    def ping(self, num_pings):
        children_outputs = []
        for _ in range(num_pings):
            children_outputs += [
                child.ping.remote() for child in self.children
            ]
        try:
            ray.get(children_outputs)
        except Exception:
            # Replace the children if one of them died.
            self.__init__(len(self.children), self.death_probability)

    def kill(self):
        # Clean up children.
        ray.get([child.__ray_terminate__.remote() for child in self.children])


@ray.remote
class Fork(object):
    def __init__(self, actor):
        self.actor = actor

    def ping(self, num_pings):
        ray.get(self.actor.ping.remote(num_pings))

num_parents = 100
num_children = 10
num_forks = 4
death_probability = 0.99

parents = [Parent.remote(num_children, death_probability) for _ in range(num_parents)]
all_forks = [[Fork.remote(parent) for parent in parents] for _ in range(num_forks)]

for i in range(100):
    start_time = time.time()
    outs = [fork.ping.remote(10) for forks in all_forks for fork in forks]
    # Wait a while for all the tasks to complete. This should trigger
    # reconstruction for any actor creation tasks that were forwarded
    # to nodes that then failed.
    ready, _ = ray.wait(
        outs,
        num_returns=len(outs),
        timeout=60 * 1000)
    assert len(ready) == len(outs)

    for j, out in enumerate(outs):
        try:
            print("getting parent", j)
            ray.get(out)
        except:
            parent = Parent.remote(num_children, death_probability)
            parents[j % len(parents)] = parent
            for forks in all_forks:
                forks[j % len(parents)] = Fork.remote(parent)
            print("Parent", j % len(parents), "restarted")

    ## Kill a parent actor with some probability.
    #exit_chance = np.random.rand()
    #if exit_chance > death_probability:
    #    parent_index = np.random.randint(len(parents))
    #    parents[parent_index].kill.remote()
    #    parents[parent_index] = Parent.remote(num_children, death_probability)

    logger.info("Finished trial {} in {}s".format(i, time.time() - start_time))
