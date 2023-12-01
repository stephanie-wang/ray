import io
import logging
from typing import Any, Optional

import ray
from ray.util.annotations import PublicAPI

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


def _create_channel_ref(
    buffer_size: int,
) -> "ray.ObjectRef":
    """
    Create a channel that can be read and written by co-located Ray processes.

    The channel has no buffer, so the writer will block until reader(s) have
    read the previous value. Only the channel creator may write to the channel.

    Args:
        buffer_size: The number of bytes to allocate for the object data and
            metadata. Writes to the channel must produce serialized data and
            metadata less than or equal to this value.
    Returns:
        Channel: A wrapper around ray.ObjectRef.
    """
    worker = ray._private.worker.global_worker
    worker.check_connected()

    value = b"0" * buffer_size

    try:
        object_ref = worker.put_object(
            value, owner_address=None, _is_experimental_mutable_object=True
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

    def __init__(self, buffer_size: Optional[int] = None):
        """
        Create a channel that can be read and written by co-located Ray processes.

        Only the caller may write to the channel. The channel has no buffer,
        so the writer will block until reader(s) have read the previous value.

        Args:
            buffer_size: The number of bytes to allocate for the object data and
                metadata. Writes to the channel must produce serialized data and
                metadata less than or equal to this value.
        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        if buffer_size is None:
            self._base_ref = None
        else:
            self._base_ref = _create_channel_ref(buffer_size)

        self.worker = ray._private.worker.global_worker
        self.worker.check_connected()

    @staticmethod
    def _from_base_ref(base_ref: "ray.ObjectRef") -> "Channel":
        chan = Channel()
        chan._base_ref = base_ref
        return chan

    def __reduce__(self):
        return self._from_base_ref, (self._base_ref,)

    def write(self, value: Any, num_readers: int):
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
        if num_readers <= 0:
            raise ValueError("``num_readers`` must be a positive integer.")

        try:
            serialized_value = self.worker.get_serialization_context().serialize(value)
        except TypeError as e:
            sio = io.StringIO()
            ray.util.inspect_serializability(value, print_file=sio)
            msg = (
                "Could not serialize the put value "
                f"{repr(value)}:\n"
                f"{sio.getvalue()}"
            )
            raise TypeError(msg) from e

        self.worker.core_worker.experimental_mutable_object_put_serialized(
            serialized_value,
            self._base_ref,
            num_readers,
        )

    def begin_read(self) -> Any:
        """
        Read the latest value from the channel. This call will block until a
        value is available to read.

        Returns:
            Any: The deserialized value.
        """
        values, _ = self.worker.get_objects(
            [self._base_ref], _is_experimental_mutable_object=True
        )
        return values[0]

    def end_read(self):
        """
        Signal to the writer that the channel is ready to write again.

        If begin_read is not called first, then this call will block until a
        value is written, then drop the value.
        """
        self.worker.core_worker.experimental_mutable_object_read_release(
            [self._base_ref]
        )
