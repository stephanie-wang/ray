import copy
import itertools
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, Iterator, List, Optional, Set, Union

import ray
from ray import ObjectRef
from ray._raylet import StreamingObjectRefGenerator
from ray.data._internal.compute import (
    ActorPoolStrategy,
    ComputeStrategy,
    TaskPoolStrategy,
)
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    OneToOneOperator,
)
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.memory_tracing import trace_allocation
from ray.data._internal.stats import StatsDict
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class MapOperator(OneToOneOperator, ABC):
    """A streaming operator that maps input bundles 1:1 to output bundles.

    This operator implements the distributed map operation, supporting both task
    and actor compute strategies.
    """

    def __init__(
        self,
        map_transformer: MapTransformer,
        input_op: PhysicalOperator,
        name: str,
        target_max_block_size: Optional[int],
        min_rows_per_bundle: Optional[int],
        ray_remote_args: Optional[Dict[str, Any]],
    ):
        # NOTE: This constructor should not be called directly; use MapOperator.create()
        # instead.
        # NOTE: This constructor must be called by subclasses.

        self._map_transformer = map_transformer
        self._ray_remote_args = _canonicalize_ray_remote_args(ray_remote_args or {})
        self._ray_remote_args_factory = None
        self._remote_args_for_metrics = copy.deepcopy(self._ray_remote_args)

        # Bundles block references up to the min_rows_per_bundle target.
        self._block_ref_bundler = _BlockRefBundler(min_rows_per_bundle)
        # Object store allocation stats.
        self._metrics = _ObjectStoreMetrics(alloc=0, freed=0, cur=0, peak=0)

        # Queue for task outputs, either ordered or unordered (this is set by start()).
        self._output_queue: _OutputQueue = None
        # Output metadata, added to on get_next().
        self._output_metadata: List[BlockMetadata] = []
        # All active `DataOpTask`s.
        self._data_tasks: Dict[int, DataOpTask] = {}
        self._next_data_task_idx = 0
        # All active `MetadataOpTask`s.
        self._metadata_tasks: Dict[int, MetadataOpTask] = {}
        self._next_metadata_task_idx = 0
        # Keep track of all finished streaming generators.
        # TODO(hchen): This is a workaround for a bug of lineage reconstruction.
        # When the streaming generator ref is GC'ed, the objects it generated
        # cannot be reconstructed. Should remove it once Ray Core fixes the bug.
        self._finished_streaming_gens: List[StreamingObjectRefGenerator] = []
        super().__init__(name, input_op, target_max_block_size)

    @property
    def actual_target_max_block_size(self) -> int:
        """
        The actual target max block size output by this operator.
        """
        target_max_block_size = self._target_max_block_size
        if target_max_block_size is None:
            target_max_block_size = DataContext.get_current().target_max_block_size

        return target_max_block_size

    @classmethod
    def create(
        cls,
        map_transformer: MapTransformer,
        input_op: PhysicalOperator,
        target_max_block_size: Optional[int],
        name: str = "Map",
        # TODO(ekl): slim down ComputeStrategy to only specify the compute
        # config and not contain implementation code.
        compute_strategy: Optional[ComputeStrategy] = None,
        min_rows_per_bundle: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> "MapOperator":
        """Create a MapOperator.

        This factory creates the MapOperator pool implementation that corresponds to the
        compute argument:
            - If None or TaskPoolStrategy -> TaskPoolMapOperator
            - If ActorPoolStrategy -> ActorPoolMapOperator

        Args:
            transform_fn: The function to apply to each ref bundle input.
            input_op: Operator generating input data for this op.
            init_fn: The callable class to instantiate if using ActorPoolMapOperator.
            name: The name of this operator.
            compute_strategy: Customize the compute strategy for this op.
            target_max_block_size: The target maximum number of bytes to
                include in an output block.
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            ray_remote_args: Customize the ray remote args for this op's tasks.
        """
        if compute_strategy is None:
            compute_strategy = TaskPoolStrategy()

        if isinstance(compute_strategy, TaskPoolStrategy):
            from ray.data._internal.execution.operators.task_pool_map_operator import (
                TaskPoolMapOperator,
            )

            return TaskPoolMapOperator(
                map_transformer,
                input_op,
                name=name,
                target_max_block_size=target_max_block_size,
                min_rows_per_bundle=min_rows_per_bundle,
                ray_remote_args=ray_remote_args,
            )
        elif isinstance(compute_strategy, ActorPoolStrategy):
            from ray.data._internal.execution.operators.actor_pool_map_operator import (
                ActorPoolMapOperator,
                AutoscalingConfig,
                AutoscalingPolicy,
            )

            autoscaling_config = AutoscalingConfig.from_compute_strategy(
                compute_strategy
            )
            autoscaling_policy = AutoscalingPolicy(autoscaling_config)

            return ActorPoolMapOperator(
                map_transformer,
                input_op,
                autoscaling_policy=autoscaling_policy,
                name=name,
                target_max_block_size=target_max_block_size,
                min_rows_per_bundle=min_rows_per_bundle,
                ray_remote_args=ray_remote_args,
            )
        else:
            raise ValueError(f"Unsupported execution strategy {compute_strategy}")

    def start(self, options: "ExecutionOptions"):
        super().start(options)
        # Create output queue with desired ordering semantics.
        if options.preserve_order:
            self._output_queue = _OrderedOutputQueue()
        else:
            self._output_queue = _UnorderedOutputQueue()

        if options.locality_with_output:
            if isinstance(options.locality_with_output, list):
                locs = options.locality_with_output
            else:
                locs = [ray.get_runtime_context().get_node_id()]

            class RoundRobinAssign:
                def __init__(self, locs):
                    self.locs = locs
                    self.i = 0

                def __call__(self, args):
                    args = copy.deepcopy(args)
                    args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                        self.locs[self.i],
                        soft=True,
                        _spill_on_unavailable=True,
                    )
                    self.i += 1
                    self.i %= len(self.locs)
                    return args

            self._ray_remote_args_factory = RoundRobinAssign(locs)

        # Put the function def in the object store to avoid repeated serialization
        # in case it's large (i.e., closure captures large objects).
        self._map_transformer_ref = ray.put(self._map_transformer)

    def add_input(self, refs: RefBundle, input_index: int):
        assert input_index == 0, input_index
        # Add ref bundle allocation to operator's object store metrics.
        self._metrics.cur += refs.size_bytes()
        if self._metrics.cur > self._metrics.peak:
            self._metrics.peak = self._metrics.cur
        # Add RefBundle to the bundler.
        self._block_ref_bundler.add_bundle(refs)
        if self._block_ref_bundler.has_bundle():
            # If the bundler has a full bundle, add it to the operator's task submission
            # queue.
            bundle = self._block_ref_bundler.get_next_bundle()
            self._add_bundled_input(bundle)

    def _get_runtime_ray_remote_args(
        self, input_bundle: Optional[RefBundle] = None
    ) -> Dict[str, Any]:
        ray_remote_args = copy.deepcopy(self._ray_remote_args)
        # For tasks with small args, we will use SPREAD by default to optimize for
        # compute load-balancing. For tasks with large args, we will use DEFAULT to
        # allow the Ray locality scheduler a chance to optimize task placement.
        if "scheduling_strategy" not in ray_remote_args:
            ctx = DataContext.get_current()
            if input_bundle and input_bundle.size_bytes() > ctx.large_args_threshold:
                ray_remote_args[
                    "scheduling_strategy"
                ] = ctx.scheduling_strategy_large_args
                # Takes precedence over small args case. This is to let users know
                # when the large args case is being triggered.
                self._remote_args_for_metrics = copy.deepcopy(ray_remote_args)
            else:
                ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy
                # Only save to metrics if we haven't already done so.
                if "scheduling_strategy" not in self._remote_args_for_metrics:
                    self._remote_args_for_metrics = copy.deepcopy(ray_remote_args)
        # This should take precedence over previously set scheduling strategy, as it
        # implements actor-based locality overrides.
        if self._ray_remote_args_factory:
            return self._ray_remote_args_factory(ray_remote_args)
        return ray_remote_args

    @abstractmethod
    def _add_bundled_input(self, refs: RefBundle):
        """Add a pre-bundled upstream output to this operator.

        Unlike the add_input() arg, this RefBundle has already been further bundled by
        _block_ref_bundler up to the target size, meaning that this bundle is ready for
        task submission.

        This must be implemented by subclasses.

        Args:
            refs: The fully-bundled ref bundle that should be added as input.
        """
        raise NotImplementedError

    def _submit_data_task(
        self,
        gen: StreamingObjectRefGenerator,
        inputs: RefBundle,
        task_done_callback: Optional[Callable[[], None]] = None,
    ):
        """Submit a new data-handling task."""
        # TODO(hchen):
        # 1. Move this to the base PhyscialOperator class.
        # 2. This method should only take a block-processing function as input,
        #    instead of a streaming generator. The logic of submitting ray tasks
        #    can also be capsulated in the base class.
        task_index = self._next_data_task_idx
        self._next_data_task_idx += 1

        def _output_ready_callback(task_index, output: RefBundle):
            # Since output is streamed, it should only contain one block.
            assert len(output.blocks) == 1
            for block_ref, _ in output.blocks:
                trace_allocation(block_ref, "map_operator_output")
            # Notify output queue that the task has produced an new output.
            self._output_queue.notify_task_output_ready(task_index, output)
            # Update object store metrics.
            allocated = output.size_bytes()
            self._metrics.alloc += allocated
            self._metrics.cur += allocated
            if self._metrics.cur > self._metrics.peak:
                self._metrics.peak = self._metrics.cur

        def _task_done_callback(task_index, inputs):
            # We should only destroy the input bundle when the whole task is done.
            # Otherwise, if the task crashes in the middle, we can't rerun it.
            inputs.destroy_if_owned()
            freed = inputs.size_bytes()
            self._metrics.freed += freed
            self._metrics.cur -= freed
            task = self._data_tasks.pop(task_index)
            self._finished_streaming_gens.append(task.get_waitable())
            # Notify output queue that this task is complete.
            self._output_queue.notify_task_completed(task_index)
            if task_done_callback:
                task_done_callback()

        self._data_tasks[task_index] = DataOpTask(
            gen,
            lambda output: _output_ready_callback(task_index, output),
            lambda: _task_done_callback(task_index, inputs),
        )

    def _submit_metadata_task(
        self, result_ref: ObjectRef, task_done_callback: Callable[[], None]
    ):
        """Submit a new metadata-handling task."""
        # TODO(hchen): Move this to the base PhyscialOperator class.
        task_index = self._next_metadata_task_idx
        self._next_metadata_task_idx += 1

        def _task_done_callback():
            self._metadata_tasks.pop(task_index)
            task_done_callback()

        self._metadata_tasks[task_index] = MetadataOpTask(
            result_ref, _task_done_callback
        )

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._metadata_tasks.values()) + list(self._data_tasks.values())

    def all_inputs_done(self):
        self._block_ref_bundler.done_adding_bundles()
        if self._block_ref_bundler.has_bundle():
            # Handle any leftover bundles in the bundler.
            bundle = self._block_ref_bundler.get_next_bundle()
            self._add_bundled_input(bundle)
        super().all_inputs_done()

    def has_next(self) -> bool:
        assert self._started
        return self._output_queue.has_next()

    def get_next(self) -> RefBundle:
        assert self._started
        bundle = self._output_queue.get_next()
        self._metrics.cur -= bundle.size_bytes()
        for _, meta in bundle.blocks:
            self._output_metadata.append(meta)
        return bundle

    @abstractmethod
    def progress_str(self) -> str:
        raise NotImplementedError

    def get_metrics(self) -> Dict[str, int]:
        sorted_ray_args = dict(sorted(self._remote_args_for_metrics.items()))
        return dict(
            self._metrics.to_metrics_dict(),
            ray_remote_args=sorted_ray_args,
        )

    def get_stats(self) -> StatsDict:
        return {self._name: self._output_metadata}

    def get_map_transformer(self) -> MapTransformer:
        return self._map_transformer

    def shutdown(self):
        self._data_tasks.clear()
        self._metadata_tasks.clear()
        self._finished_streaming_gens.clear()

    @abstractmethod
    def current_resource_usage(self) -> ExecutionResources:
        raise NotImplementedError

    @abstractmethod
    def base_resource_usage(self) -> ExecutionResources:
        raise NotImplementedError

    @abstractmethod
    def incremental_resource_usage(self) -> ExecutionResources:
        raise NotImplementedError


@dataclass
class _ObjectStoreMetrics:
    """Metrics for object store memory allocations."""

    alloc: int
    freed: int
    cur: int
    peak: int

    def to_metrics_dict(self) -> Dict[str, int]:
        return {
            "obj_store_mem_alloc": self.alloc,
            "obj_store_mem_freed": self.freed,
            "obj_store_mem_peak": self.peak,
        }


def _map_task(
    map_transformer: MapTransformer,
    target_max_block_size: int,
    data_context: DataContext,
    ctx: TaskContext,
    *blocks: Block,
) -> Iterator[Union[Block, List[BlockMetadata]]]:
    """Remote function for a single operator task.

    Args:
        fn: The callable that takes Iterator[Block] as input and returns
            Iterator[Block] as output.
        blocks: The concrete block values from the task ref bundle.

    Returns:
        A generator of blocks, followed by the list of BlockMetadata for the blocks
        as the last generator return.
    """
    DataContext._set_current(data_context)
    stats = BlockExecStats.builder()
    for b_out in map_transformer.apply_transform(
        iter(blocks), target_max_block_size, ctx
    ):
        # TODO(Clark): Add input file propagation from input blocks.
        m_out = BlockAccessor.for_block(b_out).get_metadata([], None)
        m_out.exec_stats = stats.build()
        yield b_out
        yield m_out
        stats = BlockExecStats.builder()


class _BlockRefBundler:
    """Rebundles RefBundles to get them close to a particular number of rows."""

    def __init__(self, min_rows_per_bundle: Optional[int]):
        """Creates a BlockRefBundler.

        Args:
            min_rows_per_bundle: The target number of rows per bundle. Note that we
                bundle up to this target, but only exceed it if not doing so would
                result in an empty bundle.
        """
        self._min_rows_per_bundle = min_rows_per_bundle
        self._bundle_buffer: List[RefBundle] = []
        self._bundle_buffer_size = 0
        self._finalized = False

    def add_bundle(self, bundle: RefBundle):
        """Add a bundle to the bundler."""
        self._bundle_buffer.append(bundle)
        self._bundle_buffer_size += self._get_bundle_size(bundle)

    def has_bundle(self) -> bool:
        """Returns whether the bundler has a bundle."""
        return self._bundle_buffer and (
            self._min_rows_per_bundle is None
            or self._bundle_buffer_size >= self._min_rows_per_bundle
            or (self._finalized and self._bundle_buffer_size > 0)
        )

    def get_next_bundle(self) -> RefBundle:
        """Gets the next bundle."""
        assert self.has_bundle()
        if self._min_rows_per_bundle is None:
            # Short-circuit if no bundle row target was defined.
            assert len(self._bundle_buffer) == 1
            bundle = self._bundle_buffer[0]
            self._bundle_buffer = []
            self._bundle_buffer_size = 0
            return bundle
        leftover = []
        output_buffer = []
        output_buffer_size = 0
        buffer_filled = False
        for bundle in self._bundle_buffer:
            bundle_size = self._get_bundle_size(bundle)
            if buffer_filled:
                # Buffer has been filled, save it in the leftovers.
                leftover.append(bundle)
            elif (
                output_buffer_size + bundle_size <= self._min_rows_per_bundle
                or output_buffer_size == 0
            ):
                # Bundle fits in buffer, or bundle doesn't fit but the buffer still
                # needs a non-empty bundle.
                output_buffer.append(bundle)
                output_buffer_size += bundle_size
            else:
                # Bundle doesn't fit in a buffer that already has at least one non-empty
                # bundle, so we add it to the leftovers.
                leftover.append(bundle)
                # Add all remaining bundles to the leftovers.
                buffer_filled = True
        self._bundle_buffer = leftover
        self._bundle_buffer_size = sum(
            self._get_bundle_size(bundle) for bundle in leftover
        )
        return _merge_ref_bundles(*output_buffer)

    def done_adding_bundles(self):
        """Indicate that no more RefBundles will be added to this bundler."""
        self._finalized = True

    @staticmethod
    def _get_bundle_size(bundle: RefBundle):
        return bundle.num_rows() if bundle.num_rows() is not None else float("inf")


def _merge_ref_bundles(*bundles: RefBundle) -> RefBundle:
    """Merge N ref bundles into a single bundle of multiple blocks."""
    # Check that at least one bundle is non-null.
    assert any(bundle is not None for bundle in bundles)
    blocks = list(
        itertools.chain(
            block for bundle in bundles if bundle is not None for block in bundle.blocks
        )
    )
    owns_blocks = all(bundle.owns_blocks for bundle in bundles if bundle is not None)
    return RefBundle(blocks, owns_blocks)


class _OutputQueue(ABC):
    """Interface for swapping between different output order modes."""

    @abstractmethod
    def notify_task_output_ready(self, task_index: int, output: RefBundle):
        """Called when a task's output is ready."""
        pass

    def notify_task_completed(self, task_index: int):
        """Called when a previously pending task completes."""
        pass

    @abstractmethod
    def has_next(self) -> bool:
        pass

    @abstractmethod
    def get_next(self) -> RefBundle:
        pass


class _OrderedOutputQueue(_OutputQueue):
    """An queue that returns finished tasks in submission order."""

    def __init__(self):
        self._task_outputs: Dict[int, Deque[RefBundle]] = defaultdict(lambda: deque())
        self._current_output_index: int = 0
        self._completed_tasks: Set[int] = set()

    def notify_task_output_ready(self, task_index: int, output: RefBundle):
        self._task_outputs[task_index].append(output)

    def _move_to_next_task(self):
        """Move the outut index to the next task.

        This method should only be called when the current task is complete and all
        outputs have been taken.
        """
        assert len(self._task_outputs[self._current_output_index]) == 0
        assert self._current_output_index in self._completed_tasks
        del self._task_outputs[self._current_output_index]
        self._completed_tasks.remove(self._current_output_index)
        self._current_output_index += 1

    def notify_task_completed(self, task_index: int):
        assert task_index >= self._current_output_index
        self._completed_tasks.add(task_index)
        if task_index == self._current_output_index:
            if len(self._task_outputs[task_index]) == 0:
                self._move_to_next_task()

    def has_next(self) -> bool:
        return len(self._task_outputs[self._current_output_index]) > 0

    def get_next(self) -> RefBundle:
        next_bundle = self._task_outputs[self._current_output_index].popleft()
        if len(self._task_outputs[self._current_output_index]) == 0:
            if self._current_output_index in self._completed_tasks:
                self._move_to_next_task()
        return next_bundle


class _UnorderedOutputQueue(_OutputQueue):
    """An queue that does not guarantee output order of finished tasks."""

    def __init__(self):
        self._queue: Deque[RefBundle] = deque()

    def notify_task_output_ready(self, _: int, output: RefBundle):
        self._queue.append(output)

    def has_next(self) -> bool:
        return len(self._queue) > 0

    def get_next(self) -> RefBundle:
        return self._queue.popleft()


def _canonicalize_ray_remote_args(ray_remote_args: Dict[str, Any]) -> Dict[str, Any]:
    """Enforce rules on ray remote args for map tasks.

    Namely, args must explicitly specify either CPU or GPU, not both. Disallowing
    mixed resources avoids potential starvation and deadlock issues during scheduling,
    and should not be a serious limitation for users.
    """
    ray_remote_args = ray_remote_args.copy()
    if "num_cpus" not in ray_remote_args and "num_gpus" not in ray_remote_args:
        ray_remote_args["num_cpus"] = 1
    if ray_remote_args.get("num_gpus", 0) > 0:
        if ray_remote_args.get("num_cpus", 0) != 0:
            raise ValueError(
                "It is not allowed to specify both num_cpus and num_gpus for map tasks."
            )
    elif ray_remote_args.get("num_cpus", 0) > 0:
        if ray_remote_args.get("num_gpus", 0) != 0:
            raise ValueError(
                "It is not allowed to specify both num_cpus and num_gpus for map tasks."
            )
    return ray_remote_args
