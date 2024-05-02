from typing import (
    TYPE_CHECKING,
    Tuple,
    Any,
    Optional,
    List,
    Dict,
)

try:
    import torch
except ImportError:
    torch = None

import ray.util.serialization
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.collective.collective_group import nccl_util
from ray.experimental.channel import (
    Channel,
)

import ray
from ray.util.annotations import DeveloperAPI
from ray.experimental.channel_common import (
    ChannelContext,
)


@DeveloperAPI
class TorchTensorNcclChannel(Channel):
    def __init__(
        self,
        writer: ray.actor.ActorHandle,
        readers: List[ray.actor.ActorHandle],
        typ: "TorchTensorType",
        device: Optional["torch.device"] = None,
    ):
        from ray.air._internal import torch_utils

        self._nccl_group = None

        self._writer = writer
        self._writer_rank: Optional[int] = None
        self._readers = readers
        self._reader_ranks: Optional[List[int]] = None
        self._writer_registered : bool = False
        self._reader_registered : bool = False

        self._device = device
        if self._device is None:
            # TODO(swang): Allow default device to be overridden.
            self._device = torch_utils.get_devices()[0]
        if self._device.type != "cuda":
            raise ValueError(
                f'Actor\'s default device has type "{self._device.type}", need "cuda"'
            )

        assert typ.transport == "nccl"
        self._typ = typ

        ctx = ChannelContext.get_current()
        self._nccl_group = ctx.nccl_group
        if self._nccl_group is not None:
            self._writer_rank = self._nccl_group.get_rank(self._writer)
            self._reader_ranks = [
                self._nccl_group.get_rank(reader) for reader in self._readers
            ]

            if self._writer_rank is not None and self._writer_rank == self._nccl_group.get_self_rank():
                self._writer_registered = True

            if self._reader_ranks and self._nccl_group.get_self_rank() in self._reader_ranks:
                self._reader_registered = True


    def ensure_registered_as_writer(self):
        assert self._nccl_group is not None, "Actor is not part of a NCCL group"
        return self._writer_registered, "Actor is not the writer"

    def ensure_registered_as_reader(self) -> bool:
        assert self._nccl_group is not None, "Actor is not part of a NCCL group"
        return self._reader_registered, "Actor is not a reader"

    def __reduce__(self):
        return (self.__class__, (self._writer, self._readers, self._typ, self._device))

    def write(
        self,
        wrapped: "_TorchTensorWrapper",
        readers: Optional[List[ray.actor.ActorHandle]] = None,
    ):
        value = wrapped.tensor
        if value.shape != self._typ.shape:
            raise ValueError(
                f"torch.Tensor has shape {value.shape}, expected {self._typ.shape}"
            )
        if value.dtype != self._typ.dtype:
            raise ValueError(
                f"torch.Tensor has shape {value.shape}, expected {self._typ.shape}"
            )
        if value.device != self._device:
            raise ValueError(
                f"torch.Tensor must be on the default device: {self._device}"
            )

        if readers is not None:
            raise NotImplementedError(
                "TorchTensorNcclChannel.write() not supported for dynamically "
                "passed readers."
            )

        # TODO: If there are multiple readers, can replace with a broadcast.
        for rank in self._reader_ranks:
            self._nccl_group.send(value, rank)

    def begin_read(self) -> torch.Tensor:
        # TODO(swang): Perform the NCCL recv. Pass in the source actor.
        buf = torch.zeros(self._typ.shape, dtype=self._typ.dtype, device=self._device)
        self._nccl_group.recv(buf, self._writer_rank)
        return buf

    def end_read(self) -> None:
        return

    def close(self) -> None:
        self._nccl_group.destroy()

        # TODO(swang): Set flag for downstream kernels that have already been
        # launched to the GPU to abort.


class NcclGroup:
    def __init__(
        self,
        world_size: int,
        comm_id: int,
        rank: int,
        actor_ids_to_ranks: Dict[ray.ActorID, int],
        cuda_stream: Optional[int],
    ):
        self._rank: Optional[int] = rank
        if rank is not None:
            from cupy.cuda import nccl

            self._comm = nccl.NcclCommunicator(world_size, comm_id, rank)
        else:
            # Driver does not have a rank.
            self._comm = None
        self._actor_ids_to_ranks = actor_ids_to_ranks

        self._cuda_stream = cuda_stream

        self._closed = False

    def get_rank(self, actor: ray.actor.ActorHandle) -> int:
        if actor._ray_actor_id not in self._actor_ids_to_ranks:
            raise ValueError("Actor is not in the NCCL group.")
        return self._actor_ids_to_ranks[actor._ray_actor_id]

    def get_self_rank(self) -> int:
        return self._rank

    def send(self, value: torch.Tensor, peer_rank: int):
        if self._closed:
            raise IOError("NCCL group has been destroyed.")
        # TODO(swang): Handle aysnchronous NCCL errors.
        self._comm.send(
            nccl_util.get_tensor_ptr(value),
            value.numel(),
            nccl_util.get_nccl_tensor_dtype(value),
            peer_rank,
            self._cuda_stream,
        )

    def recv(self, buf: torch.Tensor, peer_rank: int):
        if self._closed:
            raise IOError("NCCL group has been destroyed.")
        self._comm.recv(
            nccl_util.get_tensor_ptr(buf),
            buf.numel(),
            nccl_util.get_nccl_tensor_dtype(buf),
            peer_rank,
            self._cuda_stream,
        )

    def destroy(self):
        if self._closed:
            return

        self._closed = True
        self._comm.abort()
        self._comm.destroy()


def _do_init_nccl_group(self, world_size, comm_id, rank, actor_ids_to_ranks):
    assert ray.get_gpu_ids()

    ctx = ChannelContext.get_current()
    ctx.nccl_group = NcclGroup(
        world_size,
        comm_id,
        rank,
        actor_ids_to_ranks,
        torch.cuda.current_stream().cuda_stream,
    )


@DeveloperAPI
def _do_check_has_gpu(self) -> bool:
    return bool(ray.get_gpu_ids())


def _init_nccl_group(
    actors: List[ray.actor.ActorHandle],
):
    has_gpus = ray.get(
        [actor.__ray_call__.remote(_do_check_has_gpu) for actor in actors]
    )
    for has_gpu, actor in zip(has_gpus, actors):
        if not has_gpu:
            raise ValueError(
                f"Actor {actor} returns a tensor with type hint "
                'TorchTensor(transport="nccl") but actor does not have a '
                "GPU assigned by Ray."
            )

    actor_handles_to_ranks = {actor: rank for rank, actor in enumerate(actors)}
    actor_ids_to_ranks = {
        actor._ray_actor_id: rank for actor, rank in actor_handles_to_ranks.items()
    }
    assert len(actor_ids_to_ranks) == len(
        actor_handles_to_ranks
    ), "actors must be unique"

    from cupy.cuda import nccl

    comm_id = nccl.get_unique_id()

    # TODO(swang): Handle timeout errors.
    world_size = len(actors)
    ray.get(
        [
            actor.__ray_call__.remote(
                _do_init_nccl_group,
                world_size,
                comm_id,
                rank,
                actor_ids_to_ranks,
            )
            for actor, rank in actor_handles_to_ranks.items()
        ],
        timeout=30,
    )
