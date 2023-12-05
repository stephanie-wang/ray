from typing import List

import ray
import ray.experimental.channel as ray_channel


MAX_BUFFER_SIZE = int(100 * 1e6)  # 100MB


def allocate_channel(buffer_size_bytes: int = MAX_BUFFER_SIZE, num_readers: int = 1):
    if not isinstance(buffer_size_bytes, int):
        raise ValueError("buffer_size_bytes must be an integer")
    if not isinstance(num_readers, int):
        raise ValueError("num_readers must be an integer")

    return ray_channel.Channel(buffer_size_bytes, num_readers)


def do_allocate_channel(
    self, buffer_size_bytes: int = MAX_BUFFER_SIZE, num_readers: int = 1
):
    self._output_channel = allocate_channel(buffer_size_bytes)
    return self._output_channel


def do_exec_compiled_task(
    self,
    input_channels: List["ray_channel.Channel"],
    actor_method_name: str,
):
    try:
        self._input_channels = input_channels
        method = getattr(self, actor_method_name)
        while True:
            inputs = [chan.begin_read() for chan in input_channels]
            output_val = method(*inputs)

            self._output_channel.write(output_val)
            for chan in input_channels:
                chan.end_read()

    except Exception as e:
        print("Task aborted", e)
        raise


class CompiledTask:
    """Wraps the normal Ray DAGNode with some metadata."""

    def __init__(self, idx, dag_node: "ray.dag.DAGNode"):
        self.idx = idx
        self.dag_node = dag_node

        self.args = []
        self.dependent_node_idxs = []
        self.output_channel = None

    @property
    def num_readers(self):
        return len(self.dependent_node_idxs)

    def __str__(self):
        return f"""
Node: {self.dag_node}
Arguments: {self.args}
Output: {self.output_channel}
"""


class CompiledDAG:
    def __init__(self):
        # idx -> CompiledTask.
        self.idx_to_task = {}
        # DAGNode -> idx.
        self.dag_node_to_idx = {}
        # idx counter.
        self.counter = 0

        self.input_task_idx = None
        self.output_task_idx = None
        self.node_idx_to_output_channels = {}

        # Cached.
        self.dag_input_ref = None
        self.dag_output_channels = None
        self.worker_task_refs = []

    def add_node(self, node):
        idx = self.counter
        self.idx_to_task[idx] = CompiledTask(idx, node)
        self.dag_node_to_idx[node] = idx
        self.counter += 1

    def preprocess(self):
        from ray.dag import DAGNode, InputNode

        for idx, task in self.idx_to_task.items():
            task.args = task.dag_node.get_args()
            for arg in task.args:
                if isinstance(arg, DAGNode):
                    arg_idx = self.dag_node_to_idx[arg]
                    self.idx_to_task[arg_idx].dependent_node_idxs.append(idx)
            if isinstance(task.dag_node, InputNode):
                assert self.input_task_idx is None, "more than one InputNode found"
                self.input_task_idx = idx
        # TODO: Support no-input DAGs (use an empty object to signal).
        assert (
            self.input_task_idx is not None
        ), "no InputNode found, require exactly one"

        for idx, task in self.idx_to_task.items():
            if len(task.dependent_node_idxs) == 0:
                assert self.output_task_idx is None, (
                    "More than one output node found, "
                    "make sure only one node has 0 dependent tasks"
                )
                self.output_task_idx = idx

    def compiled(self):
        from ray.dag import DAGNode, InputNode, OutputNode, ClassMethodNode

        if self.dag_input_ref is not None and self.dag_output_channels is not None:
            # Driver should ray.put on input, ray.get/release on output
            return (
                self.dag_input_ref,
                self.dag_output_channels,
            )

        queue = [self.input_task_idx]
        visited = set()
        # Create output buffers
        while queue:
            cur_idx = queue.pop(0)
            if cur_idx in visited:
                continue
            visited.add(cur_idx)

            task = self.idx_to_task[cur_idx]
            # Create an output buffer on the actor.
            assert task.output_channel is None
            if isinstance(task.dag_node, ClassMethodNode):
                fn = task.dag_node._get_remote_method("__ray_call__")
                task.output_channel = ray.get(
                    fn.remote(
                        do_allocate_channel,
                        num_readers=task.num_readers,
                    )
                )
            elif isinstance(task.dag_node, InputNode):
                task.output_channel = allocate_channel(num_readers=task.num_readers)
            else:
                assert isinstance(task.dag_node, OutputNode)

            for idx in task.dependent_node_idxs:
                queue.append(idx)

        output_node = self.idx_to_task[self.output_task_idx].dag_node
        # TODO: Add an OutputNode to the end of the DAG if
        # it's not already there.
        assert isinstance(output_node, OutputNode)

        for node_idx, task in self.idx_to_task.items():
            if node_idx == self.input_task_idx:
                # We don't need to assign an actual task for the input node.
                continue

            if node_idx == self.output_task_idx:
                # We don't need to assign an actual task for the input node.
                continue

            resolved_args = []
            for arg in task.args:
                # TODO(swang): Support non-ObjectRef args.
                assert isinstance(arg, DAGNode)
                arg_idx = self.dag_node_to_idx[arg]
                arg_buffer = self.idx_to_task[arg_idx].output_channel
                assert arg_buffer is not None
                resolved_args.append(arg_buffer)

            # TODO: Assign the task with the correct input and output buffers.
            worker_fn = task.dag_node._get_remote_method("__ray_call__")
            self.worker_task_refs.append(
                worker_fn.remote(
                    do_exec_compiled_task,
                    resolved_args,
                    task.dag_node.get_method_name(),
                )
            )

        self.dag_input_ref = self.idx_to_task[self.input_task_idx].output_channel

        self.dag_output_channels = []
        for output in self.idx_to_task[self.output_task_idx].args:
            assert isinstance(output, DAGNode)
            output_idx = self.dag_node_to_idx[output]
            self.dag_output_channels.append(self.idx_to_task[output_idx].output_channel)

        assert self.dag_input_ref
        assert self.dag_output_channels
        # Driver should ray.put on input, ray.get/release on output
        return (self.dag_input_ref, self.dag_output_channels)


def build_compiled_dag(dag: "ray.dag.DAGNode"):
    compiled_dag = CompiledDAG()

    def _build_compiled_dag(node):
        compiled_dag.add_node(node)
        return node

    dag.apply_recursive(_build_compiled_dag)
    compiled_dag.preprocess()
    return compiled_dag
