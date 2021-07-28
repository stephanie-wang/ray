import os
import signal
import sys

import numpy as np
import pytest

import ray
from ray.test_utils import (
    wait_for_condition,
    wait_for_pid_to_exit,
)

def test_depth(ray_start_cluster):
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote
    def f(x):
        return x


    x = ray.put(1)
    for i in range(10):
        print(i, x)
        x = f.remote(x)
    ray.get(x)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
