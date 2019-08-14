#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
NUM_ITERATIONS=100

HEAD_IP=$1
NUM_RAYLETS=$2
OUTPUT_DIR=$3
NUM_FLOAT32=$4
USE_GCS_ONLY=$5
MAX_FAILURES=$6
GCS_DELAY_MS=$7
CHECKPOINT_INTERVAL=${8:-0}

NUM_SHARDS=1
NONDETERMINISM=0
MAX_FAILURES=1
OBJECT_STORE_MEMORY_GB=18
OBJECT_STORE_EVICTION=100
PEG=0
OBJECT_MANAGER_THREADS=4

if [[ $NUM_FLOAT32 -gt 25000000 ]]; then
  OBJECT_STORE_EVICTION=20
  NUM_ITERATIONS=20
fi

latency_prefix="latency-"$NUM_RAYLETS"-workers-"$NUM_SHARDS"-shards-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-"$NUM_FLOAT32"-bytes-"
FAILURE_ARG=""
if [[ $CHECKPOINT_INTERVAL -ne 0 ]]
then
    latency_prefix=failure-$latency_prefix
    NUM_ITERATIONS=$(( $CHECKPOINT_INTERVAL * 2 ))
    FAILURE_ARG="--test-failure --checkpoint-interval $CHECKPOINT_INTERVAL --fail-at $(( $CHECKPOINT_INTERVAL + $CHECKPOINT_INTERVAL * 9 / 10))"
fi

latency_prefix=$OUTPUT_DIR"/"$latency_prefix
if ls $latency_prefix* 1> /dev/null 2>&1
then
    echo "Latency file with prefix $latency_prefix already found, skipping..."
    exit
fi
latency_file=$latency_prefix`date +%y-%m-%d-%H-%M-%S`.txt

bash -x $DIR/start_cluster.sh \
    $NUM_RAYLETS \
    $NUM_SHARDS \
    $USE_GCS_ONLY \
    $GCS_DELAY_MS \
    $NONDETERMINISM \
    $MAX_FAILURES \
    $OBJECT_STORE_MEMORY_GB \
    $OBJECT_STORE_EVICTION \
    $PEG \
    $OBJECT_MANAGER_THREADS

echo "Logging to file $latency_file..."
cmd="python $DIR/../allreduce.py --num-workers $NUM_RAYLETS --size $NUM_FLOAT32 --num-iterations $NUM_ITERATIONS --redis-address $HEAD_IP:6379 --object-store-memory-gb $OBJECT_STORE_MEMORY_GB --latency-file $latency_file $FAILURE_ARG"
echo $cmd | tee $latency_file
$cmd 2>&1 | tee -a $latency_file

ray stop
