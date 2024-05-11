import asyncio
from collections import defaultdict
from typing import Any, Callable, Dict, List, Tuple, Union, Optional, Set
import logging
import traceback
import threading

import ray
from ray.exceptions import RayTaskError
from ray.experimental.channel import (
    _do_register_custom_serializers,
    ChannelInterface,
    ChannelOutputType,
    ReaderInterface,
    SynchronousReader,
    WriterInterface,
    SynchronousWriter,
    AwaitableBackgroundReader,
    AwaitableBackgroundWriter,
)
from ray.util.annotations import DeveloperAPI, PublicAPI

from ray.experimental.channel.shared_memory_channel import (
    SharedMemoryType,
)
from ray.experimental.channel.torch_tensor_type import (
    TorchTensorType,
    _TorchTensorWrapper,
)
from ray.experimental.channel.torch_tensor_nccl_channel import (
    _init_nccl_group,
    _destroy_nccl_group,
)

MAX_BUFFER_SIZE = int(100 * 1e6)  # 100MB

logger = logging.getLogger(__name__)


@DeveloperAPI
def do_allocate_channel(
    self,
    readers: List[Optional["ray.actor.ActorHandle"]],
    typ: ChannelOutputType,
) -> ChannelInterface:
    """Generic actor method to allocate an output channel.

    Args:
        readers: The actor handles of the readers.
        buffer_size_bytes: The maximum size of messages in the channel.

    Returns:
        The allocated channel.
    """
    self_actor = None
    try:
        self_actor = ray.get_runtime_context().current_actor
    except RuntimeError:
        # This is the driver so there is no current actor handle.
        pass

    output_channel = typ.create_channel(
        self_actor,
        readers,
    )
    return output_channel


@DeveloperAPI
def do_exec_tasks(
    self,
    tasks: List["ExecutableTask"],
    type_hints: List[type],
) -> None:
    """Generic actor method to begin executing the tasks belonging to an actor.
    This runs an infinite loop to run each task in turn (following the order specified
    in the list): reading input channel(s), executing the given taks, and writing output
    channel(s). It only exits if the actor dies or an exception is thrown.

    Args:
        tasks: the executable tasks corresponding to the actor methods.
    """
    try:
        _do_register_custom_serializers(self, type_hints)

        self._input_readers = []
        self._output_writers = []
        for task in tasks:
            _prep_task(self, task)

        done = False
        while True:
            if done:
                break
            for idx, task in enumerate(tasks):
                done = _exec_task(self, task, idx)
                if done:
                    break

    except Exception:
        logging.exception("Compiled DAG task exited with exception")
        raise


@DeveloperAPI
def do_cancel_executable_tasks(self, tasks: List["ExecutableTask"]) -> None:
    for idx in range(len(tasks)):
        self._input_readers[idx].close()
        self._output_writers[idx].close()


def _prep_task(self, task: "ExecutableTask") -> None:
    """
    Prepare the task for execution.
    """
    # Add placeholders for input channels.
    for idx, inp in enumerate(task.resolved_args):
        if isinstance(inp, ChannelInterface):
            task.input_channels.append(inp)
            task.input_channel_idxs.append(idx)
            task.resolved_inputs.append(None)
        else:
            task.resolved_inputs.append(inp)

    input_reader: ReaderInterface = SynchronousReader(task.input_channels)
    output_writer: WriterInterface = SynchronousWriter(task.output_channel)
    self._input_readers.append(input_reader)
    self._output_writers.append(output_writer)

    input_reader.start()
    output_writer.start()


def _exec_task(self, task: "ExecutableTask", idx: int) -> bool:
    """
    Execute the task.
    Args:
        task: The task to execute.
        idx: The index of the task in the list of tasks of the actor.
    Returns:
        True if we are done executing all tasks of this actor, False otherwise.
    """
    # TODO: for cases where output is passed as input to a task on
    # the same actor, introduce a "LocalChannel" to avoid the overhead
    # of serialization/deserialization and synchronization.
    method = getattr(self, task.method_name)
    input_reader = self._input_readers[idx]
    output_writer = self._output_writers[idx]
    res = None
    try:
        res = input_reader.begin_read()
    except IOError:
        # Channel closed. Exit the loop.
        return True
    except Exception as exc:
        # Previous task raised an application-level exception.
        # Propagate it and skip the actual task.
        output_writer.write(exc)
        input_reader.end_read()
        return False

    for idx, output in zip(task.input_channel_idxs, res):
        task.resolved_inputs[idx] = output

    try:
        output_val = method(*task.resolved_inputs)
        # TODO(swang): Avoid output wrapper fn by setting ChannelContext
        # instead.
        if task.output_wrapper_fn is not None:
            output_val = task.output_wrapper_fn(output_val)
    except Exception as exc:
        output_writer.write(_wrap_exception(exc))
    else:
        output_writer.write(output_val)

    try:
        input_reader.end_read()
    except IOError:
        return True
    return False


def _wrap_exception(exc):
    backtrace = ray._private.utils.format_error_message(
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        task_exception=True,
    )
    wrapped = RayTaskError(
        function_name="do_exec_tasks",
        traceback_str=backtrace,
        cause=exc,
    )
    return wrapped


@PublicAPI(stability="alpha")
class AwaitableDAGOutput:
    def __init__(self, fut: asyncio.Future, ReaderInterface: ReaderInterface):
        self._fut = fut
        self._reader = ReaderInterface

    async def begin_read(self):
        ret = await self._fut
        if isinstance(ret, Exception):
            raise ret
        return ret

    def end_read(self):
        self._reader.end_read()

    async def __aenter__(self):
        ret = await self._fut
        return ret

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.end_read()


@DeveloperAPI
class CompiledTask:
    """Wraps the normal Ray DAGNode with some metadata."""

    def __init__(self, idx: int, dag_node: "ray.dag.DAGNode"):
        """
        Args:
            idx: A unique index into the original DAG.
            dag_node: The original DAG node created by the user.
        """
        self.idx = idx
        self.dag_node = dag_node
        self.arg_idx_to_tensor_meta: Dict[int, Dict[str, Any]] = {}

        self.downstream_node_idxs: Dict[int, "ray.actor.ActorHandle"] = {}
        self.output_channel = None

        # If set, a lambda to apply to the task output. This can be used to
        # check type hints, if any.
        self.output_wrapper_fn = None
        if self.dag_node.type_hint is not None:
            if isinstance(self.dag_node.type_hint, TorchTensorType):
                # Wrap outputs produced by this task to indicate that it
                # should be specially serialized.
                self.output_wrapper_fn = lambda t: _TorchTensorWrapper(
                    t, self.dag_node.type_hint
                )

    @property
    def args(self) -> Tuple[Any]:
        return self.dag_node.get_args()

    @property
    def num_readers(self) -> int:
        return len(self.downstream_node_idxs)

    def __str__(self) -> str:
        return f"""
Node: {self.dag_node}
Arguments: {self.args}
Output: {self.output_channel}
"""


@DeveloperAPI
class ExecutableTask:
    """A task that can be executed in a compiled DAG, and it
    corresponds to an actor method.
    """

    def __init__(
        self,
        task: "CompiledTask",
        resolved_args: List[Any],
    ):
        """
        Args:
            task: The CompiledTask that this ExecutableTask corresponds to.
            resolved_args: The arguments to the method. Arguments that are
                not Channels will get passed through to the actor method.
                If the argument is a channel, it will be replaced by the
                value read from the channel before the method executes.
        """
        self.method_name = task.dag_node.get_method_name()
        self.bind_index = task.dag_node._get_bind_index()
        self.output_channel = task.output_channel
        self.output_wrapper_fn = task.output_wrapper_fn
        self.resolved_args = resolved_args

        self.resolved_inputs: List[Union[Any, ChannelInterface]] = []
        self.input_channels: List[ChannelInterface] = []
        self.input_channel_idxs: List[int] = []


@DeveloperAPI
class CompiledDAG:
    """Experimental class for accelerated execution.

    This class should not be called directly. Instead, create
    a ray.dag and call experimental_compile().

    See REP https://github.com/ray-project/enhancements/pull/48 for more
    information.
    """

    def __init__(
        self,
        buffer_size_bytes: Optional[int],
        enable_asyncio: bool = False,
        async_max_queue_size: Optional[int] = None,
    ):
        """
        Args:
            buffer_size_bytes: The number of bytes to allocate for object data and
                metadata. Each argument passed to a task in the DAG must be
                less than or equal to this value when serialized.
            enable_asyncio: Whether to enable asyncio. If enabled, caller must
                be running in an event loop and must use `execute_async` to
                invoke the DAG. Otherwise, the caller should use `execute` to
                invoke the DAG.
            async_max_queue_size: Optional parameter to limit how many DAG
                inputs can be queued at a time. The actual number of concurrent
                DAG invocations may be higher than this, if there are already
                inputs being processed by the DAG executors. If used, the
                caller is responsible for preventing deadlock, i.e. if the
                input queue is full, another asyncio task is reading from the
                DAG output.
        Returns:
            Channel: A wrapper around ray.ObjectRef.
        """
        self._buffer_size_bytes: Optional[int] = buffer_size_bytes
        if self._buffer_size_bytes is None:
            self._buffer_size_bytes = MAX_BUFFER_SIZE
        self._default_type_hint: ChannelOutputType = SharedMemoryType(
            self._buffer_size_bytes
        )
        if not isinstance(self._buffer_size_bytes, int) or self._buffer_size_bytes <= 0:
            raise ValueError(
                "`buffer_size_bytes` must be a positive integer, found "
                f"{self._buffer_size_bytes}"
            )

        self._enable_asyncio: bool = enable_asyncio
        self._fut_queue = asyncio.Queue()
        self._async_max_queue_size: Optional[int] = async_max_queue_size
        # Used to ensure that the future returned to the
        # caller corresponds to the correct DAG output. I.e.
        # order of futures added to fut_queue should match the
        # order of inputs written to the DAG.
        self._dag_submission_lock = asyncio.Lock()

        # idx -> CompiledTask.
        self.idx_to_task: Dict[int, "CompiledTask"] = {}
        # DAGNode -> idx.
        self.dag_node_to_idx: Dict["ray.dag.DAGNode", int] = {}
        # idx counter.
        self.counter: int = 0

        # Attributes that are set during preprocessing.
        # Preprocessing identifies the input node and output node.
        self.input_task_idx: Optional[int] = None
        self.input_wrapper_fn: Optional[Callable[[Any], Any]] = None
        self.output_task_idx: Optional[int] = None
        self.has_single_output: bool = False
        self.actor_task_count: Dict["ray._raylet.ActorID", int] = defaultdict(int)

        # Cached attributes that are set during compilation.
        self.dag_input_channel: Optional[ChannelInterface] = None
        self.dag_output_channels: Optional[List[ChannelInterface]] = None
        self._dag_submitter: Optional[WriterInterface] = None
        self._dag_output_fetcher: Optional[ReaderInterface] = None

        # ObjectRef for each worker's task. The task is an infinite loop that
        # repeatedly executes the method specified in the DAG.
        self.worker_task_refs: Dict["ray.actor.ActorHandle", "ray.ObjectRef"] = {}
        # Set of actors present in the DAG.
        self.actor_refs = set()
        self.actor_to_tasks: Dict[
            "ray.actor.ActorHandle", List["CompiledTask"]
        ] = defaultdict(list)
        self.actor_to_executable_tasks: Dict[
            "ray.actor.ActorHandle", List["ExecutableTask"]
        ] = {}

        # Type hints specified by the user for DAG (intermediate) outputs.
        self._type_hints = []

        # Uniquely identifies the _NCCLGroup that will be used within this DAG,
        # if any.
        self._nccl_group_id: Optional[str] = None

    def _add_node(self, node: "ray.dag.DAGNode") -> None:
        idx = self.counter
        self.idx_to_task[idx] = CompiledTask(idx, node)
        self.dag_node_to_idx[node] = idx
        self.counter += 1

    def _preprocess(self) -> None:
        """Before compiling, preprocess the DAG to build an index from task to
        upstream and downstream tasks, and to set the input and output node(s)
        of the DAG.

        This function is idempotent.
        """
        from ray.dag import (
            DAGNode,
            ClassMethodNode,
            FunctionNode,
            InputAttributeNode,
            InputNode,
            MultiOutputNode,
        )

        self.input_task_idx, self.output_task_idx = None, None
        self.actor_task_count.clear()
        self._type_hints.clear()

        nccl_actors: Set["ray.actor.ActorHandle"] = set()

        # For each task node, set its upstream and downstream task nodes.
        # Also collect the set of tasks that produce torch.tensors.
        for node_idx, task in self.idx_to_task.items():
            dag_node = task.dag_node
            if not (
                isinstance(dag_node, InputNode)
                or isinstance(dag_node, MultiOutputNode)
                or isinstance(dag_node, ClassMethodNode)
            ):
                if isinstance(dag_node, InputAttributeNode):
                    # TODO(swang): Support multi args.
                    raise NotImplementedError(
                        "Compiled DAGs currently do not support kwargs or "
                        "multiple args for InputNode"
                    )
                elif isinstance(dag_node, FunctionNode):
                    # TODO(swang): Support non-actor tasks.
                    raise NotImplementedError(
                        "Compiled DAGs currently only support actor method nodes"
                    )
                else:
                    raise ValueError(
                        f"Found unsupported node of type {type(task.dag_node)}"
                    )

            if isinstance(dag_node, ClassMethodNode):
                actor_handle = dag_node._get_actor_handle()
                if actor_handle is None:
                    raise ValueError(
                        "Compiled DAGs can only bind methods to an actor "
                        "that is already created with Actor.remote()"
                    )
                self.actor_task_count[actor_handle._actor_id] += 1

                if (
                    isinstance(dag_node.type_hint, TorchTensorType)
                    and dag_node.type_hint.transport == "nccl"
                ):
                    # Add all writers to the NCCL group.
                    nccl_actors.add(actor_handle)

            for arg_idx, arg in enumerate(task.args):
                if not isinstance(arg, DAGNode):
                    continue

                upstream_node_idx = self.dag_node_to_idx[arg]
                upstream_node = self.idx_to_task[upstream_node_idx]
                downstream_actor_handle = None
                if isinstance(task.dag_node, ClassMethodNode):
                    downstream_actor_handle = task.dag_node._get_actor_handle()
                upstream_node.downstream_node_idxs[node_idx] = downstream_actor_handle

                if (
                    isinstance(upstream_node.dag_node.type_hint, TorchTensorType)
                    and upstream_node.dag_node.type_hint.transport == "nccl"
                ):
                    # Add all readers to the NCCL group.
                    nccl_actors.add(downstream_actor_handle)

            if dag_node.type_hint is not None:
                self._type_hints.append(dag_node.type_hint)

        # Find the input node to the DAG.
        for idx, task in self.idx_to_task.items():
            if isinstance(task.dag_node, InputNode):
                assert self.input_task_idx is None, "more than one InputNode found"
                self.input_task_idx = idx
        # TODO: Support no-input DAGs (use an empty object to signal).
        if self.input_task_idx is None:
            raise NotImplementedError(
                "Compiled DAGs currently require exactly one InputNode"
            )

        # Find the (multi-)output node to the DAG.
        for idx, task in self.idx_to_task.items():
            if len(task.downstream_node_idxs) == 0:
                assert self.output_task_idx is None, "More than one output node found"
                self.output_task_idx = idx

        assert self.output_task_idx is not None
        output_node = self.idx_to_task[self.output_task_idx].dag_node
        # Add an MultiOutputNode to the end of the DAG if it's not already there.
        if not isinstance(output_node, MultiOutputNode):
            self.has_single_output = True
            output_node = MultiOutputNode([output_node])
            self._add_node(output_node)
            self.output_task_idx = self.dag_node_to_idx[output_node]
            # Preprocess one more time so that we have the right output node
            # now.
            self._preprocess()

        # If there were type hints indicating transport via NCCL, initialize
        # the NCCL group on the participating actors.
        nccl_actors = list(nccl_actors)
        if None in nccl_actors:
            raise ValueError("Driver cannot participate in the NCCL group.")
        if nccl_actors and self._nccl_group_id is None:
            self._nccl_group_id = _init_nccl_group(nccl_actors)

    def _get_or_compile(
        self,
    ) -> None:
        """Compile an execution path. This allocates channels for adjacent
        tasks to send/receive values. An infinite task is submitted to each
        actor in the DAG that repeatedly receives from input channel(s) and
        sends to output channel(s).

        This function is idempotent and will cache the previously allocated
        channels. After calling this function, _dag_submitter and
        _dag_output_fetcher will be set and can be used to invoke and fetch
        outputs for the DAG.
        """
        from ray.dag import DAGNode, InputNode, MultiOutputNode, ClassMethodNode

        if self.input_task_idx is None:
            self._preprocess()

        if self._dag_submitter is not None:
            assert self._dag_output_fetcher is not None
            return

        frontier = [self.input_task_idx]
        visited = set()
        # Create output buffers
        while frontier:
            cur_idx = frontier.pop(0)
            if cur_idx in visited:
                continue
            visited.add(cur_idx)

            task = self.idx_to_task[cur_idx]
            # Create an output buffer for the actor method.
            assert task.output_channel is None

            type_hint = task.dag_node.type_hint
            if type_hint is None:
                type_hint = self._default_type_hint
            if (
                isinstance(type_hint, TorchTensorType)
                and type_hint.transport == TorchTensorType.NCCL
            ):
                type_hint.transport_group_id = self._nccl_group_id

            if isinstance(task.dag_node, ClassMethodNode):
                readers = [self.idx_to_task[idx] for idx in task.downstream_node_idxs]
                assert len(readers) == 1

                def _get_node_id(self):
                    return ray.get_runtime_context().get_node_id()

                if isinstance(readers[0].dag_node, MultiOutputNode):
                    # This node is a multi-output node, which means that it will only be
                    # read by the driver, not an actor. Thus, we handle this case by
                    # setting `reader_handles` to `[None]`.
                    reader_handles = [None]

                    fn = task.dag_node._get_remote_method("__ray_call__")

                    actor_node = ray.get(fn.remote(_get_node_id))

                    # The driver and all actors that write outputs must be on the same
                    # node for now.
                    if actor_node != _get_node_id(self):
                        raise NotImplementedError(
                            "The driver and all actors that write outputs must be on "
                            "the same node for now."
                        )
                else:
                    reader_handles = [
                        reader.dag_node._get_actor_handle() for reader in readers
                    ]
                fn = task.dag_node._get_remote_method("__ray_call__")
                task.output_channel = ray.get(
                    fn.remote(
                        do_allocate_channel,
                        reader_handles,
                        typ=type_hint,
                    )
                )
                actor_handle = task.dag_node._get_actor_handle()
                self.actor_refs.add(actor_handle)
                self.actor_to_tasks[actor_handle].append(task)
            elif isinstance(task.dag_node, InputNode):
                readers = [self.idx_to_task[idx] for idx in task.downstream_node_idxs]
                reader_handles = []
                reader_handles_set = set()
                for reader in readers:
                    reader_handle = reader.dag_node._get_actor_handle()
                    if reader_handle in reader_handles_set:
                        raise NotImplementedError(
                            "Compiled DAGs currently do not support binding the "
                            "same input on the same actor multiple times. "
                            f"Violating actor: {reader_handle}"
                        )
                    reader_handles_set.add(reader_handle)
                    reader_handles.append(reader_handle)
                task.output_channel = do_allocate_channel(
                    self,
                    reader_handles,
                    typ=type_hint,
                )
            else:
                assert isinstance(task.dag_node, MultiOutputNode)

            for idx in task.downstream_node_idxs:
                frontier.append(idx)

        # Validate input channels for tasks that have not been visited
        for node_idx, task in self.idx_to_task.items():
            if node_idx == self.input_task_idx:
                continue
            if node_idx == self.output_task_idx:
                continue
            if node_idx not in visited:
                has_at_least_one_channel_input = False
                for arg in task.args:
                    if isinstance(arg, DAGNode):
                        has_at_least_one_channel_input = True
                if not has_at_least_one_channel_input:
                    raise ValueError(
                        "Compiled DAGs require each task to take a ray.dag.InputNode "
                        "or at least one other DAGNode as an input"
                    )

        # Create executable tasks for each actor
        for actor_handle, tasks in self.actor_to_tasks.items():
            executable_tasks = []
            worker_fn = None
            for task in tasks:
                resolved_args = []
                has_at_least_one_channel_input = False
                for arg in task.args:
                    if isinstance(arg, DAGNode):
                        arg_idx = self.dag_node_to_idx[arg]
                        arg_channel = self.idx_to_task[arg_idx].output_channel
                        assert arg_channel is not None
                        resolved_args.append(arg_channel)
                        has_at_least_one_channel_input = True
                    else:
                        resolved_args.append(arg)
                # TODO: Support no-input DAGs (use an empty object to signal).
                if not has_at_least_one_channel_input:
                    raise ValueError(
                        "Compiled DAGs require each task to take a "
                        "ray.dag.InputNode or at least one other DAGNode as an "
                        "input"
                    )
                executable_task = ExecutableTask(
                    task,
                    resolved_args,
                )
                executable_tasks.append(executable_task)
                if worker_fn is None:
                    worker_fn = task.dag_node._get_remote_method("__ray_call__")
            # Sort executable tasks based on their bind index, i.e., submission order
            # so that they will be executed in that order.
            executable_tasks.sort(key=lambda task: task.bind_index)

            self.actor_to_executable_tasks[actor_handle] = executable_tasks
            # Assign the task with the correct input and output buffers.
            self.worker_task_refs[
                task.dag_node._get_actor_handle()
            ] = worker_fn.options(concurrency_group="_ray_system").remote(
                do_exec_tasks,
                executable_tasks,
                type_hints=list(set(self._type_hints)),
            )

        # Wrapper function for inputs provided to dag.execute().
        input_task = self.idx_to_task[self.input_task_idx]
        self.input_wrapper_fn = input_task.output_wrapper_fn
        self.dag_input_channel = input_task.output_channel
        _do_register_custom_serializers(self, list(set(self._type_hints)))

        self.dag_output_channels = []
        for output in self.idx_to_task[self.output_task_idx].args:
            assert isinstance(output, DAGNode)
            output_idx = self.dag_node_to_idx[output]
            self.dag_output_channels.append(self.idx_to_task[output_idx].output_channel)

        assert self.dag_input_channel
        assert self.dag_output_channels
        assert [
            output_channel is not None for output_channel in self.dag_output_channels
        ]
        # If no MultiOutputNode was specified during the DAG creation, there is only
        # one output. Return a single output channel instead of a list of
        # channels.
        if self.has_single_output:
            assert len(self.dag_output_channels) == 1
            self.dag_output_channels = self.dag_output_channels[0]

        # Driver should ray.put on input, ray.get/release on output
        self._monitor = self._monitor_failures()
        if self._enable_asyncio:
            self._dag_submitter = AwaitableBackgroundWriter(
                self.dag_input_channel, self._async_max_queue_size
            )
            self._dag_output_fetcher = AwaitableBackgroundReader(
                self.dag_output_channels,
                self._fut_queue,
            )
        else:
            self._dag_submitter = SynchronousWriter(self.dag_input_channel)
            self._dag_output_fetcher = SynchronousReader(self.dag_output_channels)

        self._dag_submitter.start()
        self._dag_output_fetcher.start()
        return

    def _monitor_failures(self):
        outer = self

        class Monitor(threading.Thread):
            def __init__(self):
                super().__init__(daemon=True)
                self.in_teardown = False

            def wait_teardown(self):
                for actor, ref in outer.worker_task_refs.items():
                    timeout = False
                    try:
                        ray.get(ref, timeout=10)
                    except ray.exceptions.GetTimeoutError:
                        logger.warn(
                            f"Compiled DAG actor {actor} is still running 10s "
                            "after teardown(). Teardown may hang."
                        )
                        timeout = True
                    except Exception:
                        # We just want to check that the task has finished so
                        # we don't care if the actor task ended in an
                        # exception.
                        pass

                    if not timeout:
                        continue

                    try:
                        ray.get(ref)
                    except Exception:
                        pass

            def teardown(self, wait: bool):
                if self.in_teardown:
                    if wait:
                        self.wait_teardown()
                    return

                logger.info("Tearing down compiled DAG")

                outer._dag_submitter.close()
                outer._dag_output_fetcher.close()

                self.in_teardown = True
                for actor in outer.actor_refs:
                    logger.info(f"Cancelling compiled worker on actor: {actor}")
                for actor, tasks in outer.actor_to_executable_tasks.items():
                    try:
                        # TODO(swang): Suppress exceptions from actors trying to
                        # read closed channels when DAG is being torn down.
                        ray.get(
                            actor.__ray_call__.remote(do_cancel_executable_tasks, tasks)
                        )
                    except Exception:
                        logger.exception("Error cancelling worker task")
                        pass

                if outer._nccl_group_id is not None:
                    _destroy_nccl_group(outer._nccl_group_id)

                if wait:
                    logger.info("Waiting for worker tasks to exit")
                    self.wait_teardown()
                    logger.info("Teardown complete")

            def run(self):
                try:
                    ray.get(list(outer.worker_task_refs.values()))
                except Exception as e:
                    logger.debug(f"Handling exception from worker tasks: {e}")
                    if self.in_teardown:
                        return
                    self.teardown(wait=True)

        monitor = Monitor()
        monitor.start()
        return monitor

    def execute(
        self,
        *args,
        **kwargs,
    ) -> ReaderInterface:
        """Execute this DAG using the compiled execution path.

        Args:
            args: Args to the InputNode.
            kwargs: Kwargs to the InputNode. Not supported yet.

        Returns:
            A list of Channels that can be used to read the DAG result.
        """
        # These errors should already be caught during compilation, but just in
        # case.
        if len(args) != 1:
            raise NotImplementedError("Compiled DAGs support exactly one InputNode arg")
        if len(kwargs) != 0:
            raise NotImplementedError("Compiled DAGs do not support kwargs")

        if self._enable_asyncio:
            raise ValueError("Use execute_async if enable_asyncio=True")

        self._get_or_compile()

        inp = args[0]
        if self.input_wrapper_fn is not None:
            inp = self.input_wrapper_fn(inp)

        self._dag_submitter.write(inp)

        return self._dag_output_fetcher

    async def execute_async(
        self,
        *args,
        **kwargs,
    ) -> AwaitableDAGOutput:
        """Execute this DAG using the compiled execution path.

        NOTE: Not threadsafe.

        Args:
            args: Args to the InputNode.
            kwargs: Kwargs to the InputNode. Not supported yet.

        Returns:
            A list of Channels that can be used to read the DAG result.
        """
        # These errors should already be caught during compilation, but just in
        # case.
        if len(args) != 1:
            raise NotImplementedError("Compiled DAGs support exactly one InputNode arg")
        if len(kwargs) != 0:
            raise NotImplementedError("Compiled DAGs do not support kwargs")

        if not self._enable_asyncio:
            raise ValueError("Use execute if enable_asyncio=False")

        self._get_or_compile()
        async with self._dag_submission_lock:
            inp = args[0]
            if self.input_wrapper_fn is not None:
                inp = self.input_wrapper_fn(inp)

            await self._dag_submitter.write(inp)
            # Allocate a future that the caller can use to get the result.
            fut = asyncio.Future()
            await self._fut_queue.put(fut)

        return AwaitableDAGOutput(fut, self._dag_output_fetcher)

    def teardown(self):
        """Teardown and cancel all actor tasks for this DAG. After this
        function returns, the actors should be available to execute new tasks
        or compile a new DAG."""
        monitor = getattr(self, "_monitor", None)
        if monitor is not None:
            monitor.teardown(wait=True)

    def __del__(self):
        monitor = getattr(self, "_monitor", None)
        if monitor is not None:
            # Teardown asynchronously.
            # NOTE(swang): Somehow, this can get called after the CoreWorker
            # has already been destructed, so it is not safe to block in
            # ray.get.
            monitor.teardown(wait=False)


@DeveloperAPI
def build_compiled_dag_from_ray_dag(
    dag: "ray.dag.DAGNode",
    buffer_size_bytes: Optional[int],
    enable_asyncio: bool = False,
    async_max_queue_size: Optional[int] = None,
) -> "CompiledDAG":
    compiled_dag = CompiledDAG(
        buffer_size_bytes,
        enable_asyncio,
        async_max_queue_size,
    )

    def _build_compiled_dag(node):
        compiled_dag._add_node(node)
        return node

    dag.apply_recursive(_build_compiled_dag)
    compiled_dag._get_or_compile()
    return compiled_dag
