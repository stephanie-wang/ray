import tempfile
from typing import List
import time

import pandas

import ray
from ray import workflow
from ray.data.dataset_pipeline import DatasetPipeline
from ray.data.datasource.datasource import RandomIntRowDatasource


SIZE_100MiB = 100 * 1024 * 1024
NUM_COLUMNS = 4

def generate_example_files(size_bytes: int) -> str:
    tmpdir = tempfile.mkdtemp()
    ray.data.read_datasource(
	RandomIntRowDatasource(),
	n=size_bytes // 8 // NUM_COLUMNS,
	num_columns=NUM_COLUMNS).write_parquet(tmpdir)
    return tmpdir

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

        self.dataset_iter = enumerate(self.shard.iter_datasets())
        self.batch_iter = None

    def train_batch(self):
        try:
            i, batch = next(self.batch_iter)
        except StopIteration:
            return None

        # TODO: replace the code for real training.
        return len(batch)


    def train_epoch(self):
        try:
            epoch, training_dataset = next(self.dataset_iter)
        except StopIteration:
            return None

        print(f"Training... worker: {self.rank}, epoch: {epoch}")
        self.batch_iter = enumerate(training_dataset.iter_batches())
        result = 0
        while True:
            r = self.train_batch()
            if r is None:
                break
            result += r
        return result


    def train(self):
        result = 0
        while True:
            r = self.train_epoch()
            if r is None:
                break
            result += r
        return result


def init_actors(pipes: List[DatasetPipeline]):
    return [TrainingWorker.remote(rank, shard) for rank, shard in enumerate(pipes)]

def train(state, workers):
    results = ray.get([worker.train.remote() for worker in workers])
    state += sum(results)
    return state

def train_epoch(state, workers):
    results = ray.get([worker.train_epoch.remote() for worker in workers])
    if results and results[0] is None:
        return state
    state += sum(results)
    return train_epoch(state, workers)

def train_batch(state, workers):
    results = ray.get([worker.train_batch.remote() for worker in workers])
    if results and results[0] is None:
        return state
    state += sum(results)
    return train_batch(state, workers)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-workflow", action="store_true")
    parser.add_argument("--train-by-batch", action="store_true")
    parser.add_argument("--train-by-epoch", action="store_true")
    parser.add_argument("--dataset-size", default=SIZE_100MiB)
    parser.add_argument("--num-epochs", default=3)
    parser.add_argument("--num-trainers", default=2)
    args = parser.parse_args()

    start = time.time()
    if not args.no_workflow:
        generate_example_files = workflow.step(generate_example_files).step
        create_shuffle_pipeline = workflow.step(create_shuffle_pipeline).step
        init_actors = workflow.step(init_actors).step
        train = workflow.step(train).step
        train_batch = workflow.step(train_batch).step
        train_epoch = workflow.step(train_epoch).step

        workflow.init()

    tmpdir = generate_example_files(args.dataset_size)
    pipes = create_shuffle_pipeline(tmpdir, args.num_epochs, args.num_trainers)
    actors = init_actors(pipes)
    # Split workflow into multiple granularities: job, epoch, batch.
    if args.train_by_epoch:
        assert not args.train_by_batch
        output = train_epoch(0, actors)
    elif args.train_by_batch:
        output = train_batch(0, actors)
    else:
        output = train(0, actors)

    if not args.no_workflow:
        output = output.run()

    print(output)
    end = time.time()
    print(f"Took {end - start}, with {'no ' if args.no_workflow else ''}workflow.")
