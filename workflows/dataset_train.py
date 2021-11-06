import tempfile
from typing import List

import pandas
import pyarrow

import ray
from ray import workflow
from ray.data.dataset_pipeline import DatasetPipeline
from ray.data.datasource.datasource import RandomIntRowDatasource


NUM_EPOCHS = 3
NUM_TRAINING_WORKERS = 3
SIZE_100MiB = 100 * 1024 * 1024
NUM_COLUMNS = 4


@workflow.step
def generate_example_files(size_bytes: int) -> str:
    tmpdir = tempfile.mkdtemp()
    ray.data.read_datasource(
	RandomIntRowDatasource(),
	n=size_bytes // 8 // NUM_COLUMNS,
	num_columns=NUM_COLUMNS).write_parquet(tmpdir)
    return tmpdir

@workflow.step
def create_shuffle_pipeline(training_data_dir: str, num_epochs: int,
                            num_shards: int) -> List[DatasetPipeline]:

    return ray.data.read_parquet(training_data_dir) \
        .repeat(num_epochs) \
        .random_shuffle_each_window() \
        .split(num_shards, equal=True)

@ray.remote
class TrainingWorker:
    def __init__(self, rank: int, shard: DatasetPipeline):
        self.rank = rank
        self.shard = shard

        self.num_rows = 0

    def train(self):
        for epoch, training_dataset in enumerate(self.shard.iter_datasets()):
            # Following code emulates epoch based SGD training.
            print(f"Training... worker: {self.rank}, epoch: {epoch}")
            for i, batch in enumerate(training_dataset.iter_batches(batch_format="pandas")):
                # TODO: replace the code for real training.
                self.num_rows += len(batch)
        return self.num_rows


@workflow.step
def init_actors(pipes: List[DatasetPipeline]):
    return [TrainingWorker.remote(rank, shard) for rank, shard in enumerate(pipes)]

@workflow.step
def train(state, workers):
    results = ray.get([worker.train.remote() for worker in workers])
    state += sum(results)
    return state


workflow.init()
tmpdir = generate_example_files.step(SIZE_100MiB)
pipes = create_shuffle_pipeline.step(tmpdir, NUM_EPOCHS, NUM_TRAINING_WORKERS)
actors = init_actors.step(pipes)
output = train.step(0, actors)
print(output.run())
