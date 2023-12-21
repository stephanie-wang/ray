# coding: utf-8
import logging
import os
import sys
import copy
import time

import numpy as np
import pytest

import ray
import ray.cluster_utils
import ray.experimental.channel as ray_channel

logger = logging.getLogger(__name__)


@pytest.mark.skipif(sys.platform != "linux", reason="Requires Linux.")
def test_put_local_get(ray_start_regular):
    chan = ray_channel.Channel(1000)

    num_writes = 1000
    for i in range(num_writes):
        val = i.to_bytes(8, "little")
        chan.write(val, num_readers=1)
        assert chan.begin_read() == val

        # Begin read multiple times will return the same value.
        assert chan.begin_read() == val

        chan.end_read()


@pytest.mark.skipif(sys.platform != "linux", reason="Requires Linux.")
def test_errors(ray_start_regular):
    @ray.remote
    class Actor:
        def make_chan(self, do_write=True):
            self.chan = ray_channel.Channel(1000)
            if do_write:
                self.chan.write(b"hello", num_readers=1)
            return self.chan

    a = Actor.remote()
    # Only original creator can write.
    chan = ray.get(a.make_chan.remote(do_write=False))
    with pytest.raises(ray.exceptions.RaySystemError):
        chan.write(b"hi")

    # Only original creator can write.
    chan = ray.get(a.make_chan.remote(do_write=True))
    assert chan.begin_read() == b"hello"
    with pytest.raises(ray.exceptions.RaySystemError):
        chan.write(b"hi")

    # Multiple consecutive reads from the same process are fine.
    chan = ray.get(a.make_chan.remote(do_write=True))
    assert chan.begin_read() == b"hello"
    assert chan.begin_read() == b"hello"
    chan.end_read()

    @ray.remote
    class Reader:
        def __init__(self):
            pass

        def read(self, chan):
            return chan.begin_read()

    # Multiple reads from n different processes, where n > num_readers, errors.
    chan = ray.get(a.make_chan.remote(do_write=True))
    readers = [Reader.remote(), Reader.remote()]
    # At least 1 reader
    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ray.get([reader.read.remote(chan) for reader in readers])
    assert "ray.exceptions.RaySystemError" in str(exc_info.value)


@pytest.mark.skipif(sys.platform != "linux", reason="Requires Linux.")
def test_put_different_meta(ray_start_regular):
    chan = ray_channel.Channel(1000)

    def _test(val):
        chan.write(val, num_readers=1)

        read_val = chan.begin_read()
        if isinstance(val, np.ndarray):
            assert np.array_equal(read_val, val)
        else:
            assert read_val == val
        chan.end_read()

    _test(b"hello")
    _test("hello")
    _test(1000)
    _test(np.random.rand(10))

    # Cannot put a serialized value larger than the allocated buffer.
    with pytest.raises(ValueError):
        _test(np.random.rand(100))

    _test(np.random.rand(1))


@pytest.mark.skipif(sys.platform != "linux", reason="Requires Linux.")
@pytest.mark.parametrize("num_readers", [1, 4])
def test_put_remote_get(ray_start_regular, num_readers):
    chan = ray_channel.Channel(1000)

    @ray.remote(num_cpus=0)
    class Reader:
        def __init__(self):
            pass

        def read(self, chan, num_writes):
            for i in range(num_writes):
                val = i.to_bytes(8, "little")
                assert chan.begin_read() == val
                chan.end_read()

            for i in range(num_writes):
                val = i.to_bytes(100, "little")
                assert chan.begin_read() == val
                chan.end_read()

            for val in [
                b"hello world",
                "hello again",
                1000,
            ]:
                assert chan.begin_read() == val
                chan.end_read()

    num_writes = 1000
    readers = [Reader.remote() for _ in range(num_readers)]
    done = [reader.read.remote(chan, num_writes) for reader in readers]
    for i in range(num_writes):
        val = i.to_bytes(8, "little")
        chan.write(val, num_readers=num_readers)

    # Test different data size.
    for i in range(num_writes):
        val = i.to_bytes(100, "little")
        chan.write(val, num_readers=num_readers)

    # Test different metadata.
    for val in [
        b"hello world",
        "hello again",
        1000,
    ]:
        chan.write(val, num_readers=num_readers)

    ray.get(done)


@pytest.mark.parametrize("remote", [True, False])
def test_remote_reader(ray_start_cluster, remote):
    num_readers = 2
    cluster = ray_start_cluster
    if remote:
        cluster.add_node(num_cpus=0)
        ray.init(address=cluster.address)
        cluster.add_node(num_cpus=num_readers)
    else:
        cluster.add_node(num_cpus=num_readers)
        ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class Reader:
        def __init__(self):
            pass

        def get_node_id(self) -> str:
            return ray.get_runtime_context().get_node_id()

        def allocate_local_reader_channel(self, writer_channel, num_readers):
            return ray_channel.Channel(_writer_channel=writer_channel,
                num_readers=num_readers)

        def send_channel(self, reader_channel):
            self._reader_chan = reader_channel

        def read(self):
            while True:
                self._reader_chan.begin_read()
                self._reader_chan.end_read()
            #values = []
            #for i in range(num_vals):
            #    print("read value x", i)
            #    values.append(copy.deepcopy(self._reader_chan.begin_read()))
            #    print("read value y", i)
            #    self._reader_chan.end_read()
            #print("READER HERE")
            #return values

    readers = [Reader.remote() for _ in range(num_readers)]
    if remote:
        reader_node_id = ray.get(readers[0].get_node_id.remote())
        channel = ray_channel.Channel(1000, _reader_node_id=reader_node_id)
        reader_channel = ray.get(
                readers[0].allocate_local_reader_channel.remote(channel, num_readers))
    else:
        channel = ray_channel.Channel(1000, num_readers=num_readers)
        reader_channel = channel

    ray.get([
        reader.send_channel.remote(reader_channel) for reader in readers])

    work = [reader.read.remote() for reader in readers]

    for _ in range(3):
        start = time.perf_counter()
        for _ in range(1000):
            channel.write(b"x")
        end = time.perf_counter()
        print(end - start, 10_000 / (end - start))

    #write_vals = [b"hello", "world", 1]
    ##write_vals = [b"hello"]
    #read_values = reader.read.remote(len(write_vals))
    #for val in write_vals:
    #    channel.write(val)
    #assert ray.get(read_values) == write_vals, read_values


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
