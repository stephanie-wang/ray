#!/bin/bash

NUM_REDIS_SHARDS="${NUM_REDIS_SHARDS:-"1"}"

for NUM_NODES in "$@";
do
    echo "Running lineage stash trials, with $NUM_NODES nodes"
    ./run-trial.sh $NUM_NODES $NUM_REDIS_SHARDS
done

for NUM_NODES in "$@";
do
    echo "Running GCS-only trials, with $NUM_NODES nodes"
    ./run-trial.sh $NUM_NODES $NUM_REDIS_SHARDS 0
done
