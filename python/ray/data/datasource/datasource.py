from typing import TYPE_CHECKING, Callable, Iterable, List, Optional, Union

import numpy as np

from ray.data._internal.util import _check_pyarrow_version, unify_block_metadata_schema
from ray.data.block import Block, BlockMetadata
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI
class Datasource:
    """Interface for defining a custom :class:`~ray.data.Dataset` datasource.

    To read a datasource into a dataset, use :meth:`~ray.data.read_datasource`.
    """  # noqa: E501

    @Deprecated
    def create_reader(self, **read_args) -> "Reader":
        """
        Deprecated: Implement :meth:`~ray.data.Datasource.get_read_tasks` and
        :meth:`~ray.data.Datasource.estimate_inmemory_data_size` instead.
        """
        return _LegacyDatasourceReader(self, **read_args)

    @Deprecated
    def prepare_read(self, parallelism: int, **read_args) -> List["ReadTask"]:
        """
        Deprecated: Implement :meth:`~ray.data.Datasource.get_read_tasks` and
        :meth:`~ray.data.Datasource.estimate_inmemory_data_size` instead.
        """
        raise NotImplementedError

    def get_name(self) -> str:
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        """
        name = type(self).__name__
        datasource_suffix = "Datasource"
        if name.endswith(datasource_suffix):
            name = name[: -len(datasource_suffix)]
        return name

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown.

        Note that the in-memory data size may be larger than the on-disk data size.
        """
        raise NotImplementedError

    def get_read_tasks(self, parallelism: int) -> List["ReadTask"]:
        """Execute the read and return read tasks.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should equal to this value if possible.

        Returns:
            A list of read tasks that can be executed to read blocks from the
            datasource in parallel.
        """
        raise NotImplementedError

    @property
    def should_create_reader(self) -> bool:
        has_implemented_get_read_tasks = (
            type(self).get_read_tasks is not Datasource.get_read_tasks
        )
        has_implemented_estimate_inmemory_data_size = (
            type(self).estimate_inmemory_data_size
            is not Datasource.estimate_inmemory_data_size
        )
        return (
            not has_implemented_get_read_tasks
            or not has_implemented_estimate_inmemory_data_size
        )

    @property
    def supports_distributed_reads(self) -> bool:
        """If ``False``, only launch read tasks on the driver's node."""
        return True

    def num_rows(self) -> Optional[int]:
        """Return the number of rows in the datasource, or ``None`` if unknown."""
        # Legacy datasources might not implement `get_read_tasks`.
        if self.should_create_reader:
            return None

        read_tasks = self.get_read_tasks(1)
        assert len(read_tasks) > 0, "Datasource must return at least one read task"
        # `get_read_tasks` isn't guaranteed to return exactly one read task.
        metadata = [read_task.get_metadata() for read_task in read_tasks]
        if all(meta.num_rows is not None for meta in metadata):
            return sum(meta.num_rows for meta in metadata)
        else:
            return None

    def schema(self) -> Optional[Union[type, "pyarrow.lib.Schema"]]:
        """Return the schema of the datasource, or ``None`` if unknown."""
        # Legacy datasources might not implement `get_read_tasks`.
        if self.should_create_reader:
            return None

        read_tasks = self.get_read_tasks(1)
        assert len(read_tasks) > 0, "Datasource must return at least one read task"
        # `get_read_tasks` isn't guaranteed to return exactly one read task.
        metadata = [read_task.get_metadata() for read_task in read_tasks]
        return unify_block_metadata_schema(metadata)

    def input_files(self) -> Optional[List[str]]:
        """Return a list of input files, or ``None`` if unknown."""
        return None


@Deprecated
class Reader:
    """A bound read operation for a :class:`~ray.data.Datasource`.

    This is a stateful class so that reads can be prepared in multiple stages.
    For example, it is useful for :class:`Datasets <ray.data.Dataset>` to know the
    in-memory size of the read prior to executing it.
    """

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown.

        Note that the in-memory data size may be larger than the on-disk data size.
        """
        raise NotImplementedError

    def get_read_tasks(self, parallelism: int) -> List["ReadTask"]:
        """Execute the read and return read tasks.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should equal to this value if possible.
            read_args: Additional kwargs to pass to the datasource impl.

        Returns:
            A list of read tasks that can be executed to read blocks from the
            datasource in parallel.
        """
        raise NotImplementedError


class _LegacyDatasourceReader(Reader):
    def __init__(self, datasource: Datasource, **read_args):
        self._datasource = datasource
        self._read_args = read_args

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(self, parallelism: int) -> List["ReadTask"]:
        return self._datasource.prepare_read(parallelism, **self._read_args)


@DeveloperAPI
class ReadTask(Callable[[], Iterable[Block]]):
    """A function used to read blocks from the :class:`~ray.data.Dataset`.

    Read tasks are generated by :meth:`~ray.data.Datasource.get_read_tasks`,
    and return a list of ``ray.data.Block`` when called. Initial metadata about the read
    operation can be retrieved via ``get_metadata()`` prior to executing the
    read. Final metadata is returned after the read along with the blocks.

    Ray will execute read tasks in remote functions to parallelize execution.
    Note that the number of blocks returned can vary at runtime. For example,
    if a task is reading a single large file it can return multiple blocks to
    avoid running out of memory during the read.

    The initial metadata should reflect all the blocks returned by the read,
    e.g., if the metadata says ``num_rows=1000``, the read can return a single
    block of 1000 rows, or multiple blocks with 1000 rows altogether.

    The final metadata (returned with the actual block) reflects the exact
    contents of the block itself.
    """

    def __init__(self, read_fn: Callable[[], Iterable[Block]], metadata: BlockMetadata):
        self._metadata = metadata
        self._read_fn = read_fn

    def get_metadata(self) -> BlockMetadata:
        return self._metadata

    def __call__(self) -> Iterable[Block]:
        result = self._read_fn()
        if not hasattr(result, "__iter__"):
            DeprecationWarning(
                "Read function must return Iterable[Block], got {}. "
                "Probably you need to return `[block]` instead of "
                "`block`.".format(result)
            )
        yield from result


@DeveloperAPI
class RandomIntRowDatasource(Datasource):
    """An example datasource that generates rows with random int64 columns.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import RandomIntRowDatasource
        >>> source = RandomIntRowDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource( # doctest: +SKIP
        ...     source, n=10, num_columns=2).take()
        {'c_0': 1717767200176864416, 'c_1': 999657309586757214}
        {'c_0': 4983608804013926748, 'c_1': 1160140066899844087}
    """

    def __init__(self, n: int, num_columns: int):
        self._n = n
        self._num_columns = num_columns

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._n * self._num_columns * 8

    def get_read_tasks(
        self,
        parallelism: int,
    ) -> List[ReadTask]:
        _check_pyarrow_version()
        import pyarrow

        read_tasks: List[ReadTask] = []
        n = self._n
        num_columns = self._num_columns
        block_size = max(1, n // parallelism)

        def make_block(count: int, num_columns: int) -> Block:
            return pyarrow.Table.from_arrays(
                np.random.randint(
                    np.iinfo(np.int64).max, size=(num_columns, count), dtype=np.int64
                ),
                names=[f"c_{i}" for i in range(num_columns)],
            )

        schema = pyarrow.Table.from_pydict(
            {f"c_{i}": [0] for i in range(num_columns)}
        ).schema

        i = 0
        while i < n:
            count = min(block_size, n - i)
            meta = BlockMetadata(
                num_rows=count,
                size_bytes=8 * count * num_columns,
                schema=schema,
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda count=count, num_columns=num_columns: [
                        make_block(count, num_columns)
                    ],
                    meta,
                )
            )
            i += block_size

        return read_tasks

    def get_name(self) -> str:
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        Note: overrides the base `Datasource` method.
        """
        return "RandomInt"
