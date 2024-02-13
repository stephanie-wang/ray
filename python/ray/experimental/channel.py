import asyncio
import io
import logging
import queue
import threading
from typing import Any, List, Optional

import ray
from ray.util.annotations import PublicAPI

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


def _create_channel_ref(
    buffer_size_bytes: int,
) -> "ray.ObjectRef":
    """
    Create a channel that can be read and written by co-located Ray processes.

    The channel has no buffer, so the writer will block until reader(s) have
    read the previous value. Only the channel creator may write to the channel.

    Args:
        buffer_size_bytes: The number of bytes to allocate for the object data and
            metadata. Writes to the channel must produce serialized data and
            metadata less than or equal to this value.
    Returns:
        Channel: A wrapper around ray.ObjectRef.
    """
    worker = ray._private.worker.global_worker
    worker.check_connected()

    value = b"0" * buffer_size_bytes

    try:
        object_ref = worker.put_object(
            value, owner_address=None, _is_experimental_channel=True
        )
    except ray.exceptions.ObjectStoreFullError:
        logger.info(
            "Put failed since the value was either too large or the "
            "store was full of pinned objects."
        )
        raise
    return object_ref


@PublicAPI(stability="alpha")
class Channel:
    """
    A wrapper type for ray.ObjectRef. Currently supports ray.get but not
    ray.wait.
    """

    def __init__(
        self,
        buffer_size_bytes: Optional[int] = None,
        num_readers: int = 1,
        _base_ref: Optional["ray.ObjectRef"] = None,
    ):
        """
        Create a channel that can be read and written by co-located Ray processes.

        Only the caller may write to the channel. The channel has no buffer,
        so the writer will block until reader(s) have read the previous value.

        Args:
            buffer_size_bytes: The number of bytes to allocate for the object data and
                metadata. Writes to the channel must produce serialized data and
                metadata less than or equal to this value.
        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        if buffer_size_bytes is None:
            if _base_ref is None:
                raise ValueError(
                    "One of `buffer_size_bytes` or `_base_ref` must be provided"
                )
            self._base_ref = _base_ref
        else:
            if not isinstance(buffer_size_bytes, int):
                raise ValueError("buffer_size_bytes must be an integer")
            self._base_ref = _create_channel_ref(buffer_size_bytes)

        if not isinstance(num_readers, int):
            raise ValueError("num_readers must be an integer")

        self._num_readers = num_readers
        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()

        self._writer_registered = False
        self._reader_registered = False

    def _ensure_registered_as_writer(self):
        if self._writer_registered:
            return

        self._worker.core_worker.experimental_channel_register_writer(self._base_ref)
        self._writer_registered = True

    def _ensure_registered_as_reader(self):
        if self._reader_registered:
            return

        self._worker.core_worker.experimental_channel_register_reader(self._base_ref)
        self._reader_registered = True

    @staticmethod
    def _from_base_ref(base_ref: "ray.ObjectRef", num_readers: int) -> "Channel":
        return Channel(num_readers=num_readers, _base_ref=base_ref)

    def __reduce__(self):
        return self._from_base_ref, (self._base_ref, self._num_readers)

    def write(self, value: Any, num_readers: Optional[int] = None):
        """
        Write a value to the channel.

        Blocks if there are still pending readers for the previous value. The
        writer may not write again until the specified number of readers have
        called ``end_read_channel``.

        Args:
            value: The value to write.
            num_readers: The number of readers that must read and release the value
                before we can write again.
        """
        if num_readers is None:
            num_readers = self._num_readers
        if num_readers <= 0:
            raise ValueError("``num_readers`` must be a positive integer.")

        self._ensure_registered_as_writer()

        try:
            serialized_value = self._worker.get_serialization_context().serialize(value)
        except TypeError as e:
            sio = io.StringIO()
            ray.util.inspect_serializability(value, print_file=sio)
            msg = (
                "Could not serialize the put value "
                f"{repr(value)}:\n"
                f"{sio.getvalue()}"
            )
            raise TypeError(msg) from e

        self._worker.core_worker.experimental_channel_put_serialized(
            serialized_value,
            self._base_ref,
            num_readers,
        )

    def begin_read(self) -> Any:
        """
        Read the latest value from the channel. This call will block until a
        value is available to read.

        Subsequent calls to begin_read() will return the same value, until
        end_read() is called. Then, the client must begin_read() again to get
        the next value.

        Returns:
            Any: The deserialized value.
        """
        self._ensure_registered_as_reader()
        return ray.get(self._base_ref)

    def end_read(self):
        """
        Signal to the writer that the channel is ready to write again.

        If begin_read is not called first, then this call will block until a
        value is written, then drop the value.
        """
        self._ensure_registered_as_reader()
        self._worker.core_worker.experimental_channel_read_release([self._base_ref])

    def close(self) -> None:
        """
        Close this channel by setting the error bit on the object.

        Does not block. Any existing values in the channel may be lost after the
        channel is closed.
        """
        logger.debug(f"Setting error bit on channel: {self._base_ref}")
        self._ensure_registered_as_writer()
        self._worker.core_worker.experimental_channel_set_error(self._base_ref)


# Interfaces for channel I/O.
class InputReader:
    def __init__(self, input_channels: List[Channel]):
        if isinstance(input_channels, List):
            for chan in input_channels:
                assert isinstance(chan, Channel)
            self._has_single_output = False
        else:
            assert isinstance(input_channels, Channel)
            self._has_single_output = True
            input_channels = [input_channels]

        self._input_channels = input_channels
        self._closed = False
        self._num_reads = 0

    def get_num_reads(self) -> int:
        return self._num_reads

    def start(self):
        raise NotImplementedError

    def _begin_read_list(self) -> Any:
        raise NotImplementedError

    def begin_read(self) -> Any:
        outputs = self._begin_read_list()
        self._num_reads += 1
        if self._has_single_output:
            return outputs[0]
        else:
            return outputs

    def end_read(self) -> Any:
        raise NotImplementedError

    def close(self) -> None:
        self._closed = True
        for channel in self._input_channels:
            channel.close()


class SynchronousInputReader(InputReader):
    def __init__(self, input_channels: List[Channel]):
        super().__init__(input_channels)

    def start(self):
        pass

    def _begin_read_list(self) -> Any:
        return [c.begin_read() for c in self._input_channels]

    def end_read(self) -> Any:
        for c in self._input_channels:
            c.end_read()


class AwaitableBackgroundInputReader(InputReader):
    def __init__(self, input_channels: List[Channel]):
        super().__init__(input_channels)
        self.queue = asyncio.Queue()
        self.background_task = None

    def start(self):
        self.background_task = self.run()
        asyncio.ensure_future(self.background_task)

    def _run(self):
        return [c.begin_read() for c in self._input_channels]

    async def run(self):
        loop = asyncio.get_event_loop()
        while not self._closed:
            try:
                res = await loop.run_in_executor(None, self._run)
            except Exception as exc:
                res = exc

            await self.queue.put(res)

    async def begin_read(self) -> Any:
        outputs = await self._begin_read_list()
        self._num_reads += 1
        if self._has_single_output:
            return outputs[0]
        else:
            return outputs

    async def _begin_read_list(self) -> Any:
        val = await self.queue.get()
        if isinstance(val, Exception):
            raise val

        return val

    def end_read(self) -> Any:
        for c in self._input_channels:
            c.end_read()


class OutputWriter:
    def __init__(self, output_channel: Channel):
        self._output_channel = output_channel
        self._closed = False
        self._num_writes = 0

    def get_num_writes(self) -> int:
        return self._num_writes

    def start(self):
        raise NotImplementedError()

    def write(self, val: Any) -> None:
        raise NotImplementedError()

    def close(self) -> None:
        self._closed = True
        self._output_channel.close()


class SynchronousOutputWriter(OutputWriter):
    def start(self):
        pass

    def write(self, val: Any) -> None:
        self._output_channel.write(val)
        self._num_writes += 1


class AwaitableBackgroundOutputWriter(OutputWriter):
    def __init__(self, output_channel: Channel, max_queue_size: int):
        super().__init__(output_channel)
        self.queue = asyncio.Queue(max_queue_size)
        self.background_task = None

    def start(self):
        self.background_task = self.run()
        asyncio.ensure_future(self.background_task)

    def _run(self, res):
        self._output_channel.write(res)

    async def run(self):
        loop = asyncio.get_event_loop()
        while True:
            res = await self.queue.get()
            await loop.run_in_executor(None, self._run, res)

    async def write(self, val: Any) -> None:
        if self._closed:
            raise RuntimeError("DAG execution cancelled")
        await self.queue.put(val)
        self._num_writes += 1
