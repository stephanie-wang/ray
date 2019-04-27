#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

HEAD_IP=$1
NUM_RAYLETS=$2
BATCH_SIZE=${3:-1000}
THROUGHPUT=${4:--1}
TEST_FAILURE=${5:-0}
NUM_STRAGGLERS=${6:-0}
NUM_SHARDS=8

if [[ $# -gt 6 || $# -lt 2 ]]
then
    echo "Usage: ./run_job.sh <head ip> <num raylets> <batch size> <target tput> <failure?> <num stragglers?>"
    exit
fi


latency_prefix=latency-$NUM_RAYLETS-workers-$NUM_SHARDS-shards-$BATCH_SIZE-batch-$THROUGHPUT-tput-
DURATION=60

FAILURE_ARGS=""
if [[ $TEST_FAILURE -ne 0 ]]
then
    CHECKPOINT_DURATION=30
    DURATION=120
    FAILURE_ARGS="--checkpoint-interval $(( $THROUGHPUT / $NUM_RAYLETS * $CHECKPOINT_DURATION )) --num-mapper-failures 1 --fail-at 42"
    latency_prefix=failure-$latency_prefix$CHECKPOINT_DURATION-checkpoint-
fi

NUM_RECORDS=1000000
if [[ $THROUGHPUT -ne -1 ]]
then
    NUM_RECORDS=$(( $THROUGHPUT * $DURATION ))
fi

if [[ $NUM_STRAGGLERS -ne 0 ]]
then
    latency_prefix=$latency_prefix$NUM_STRAGGLERS-stragglers-
fi

#if ls $latency_prefix* 1> /dev/null 2>&1
#then
#    echo "Latency file with prefix $latency_prefix already found, skipping..."
#    continue
#fi
latency_file=$latency_prefix`date +%h-%d-%M-%S`.txt
echo "Logging to file $latency_file..."


bash -x $DIR/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS

if [[ $NUM_STRAGGLERS -ne 0 ]]
then
    bash -x $DIR/add_stragglers.sh $NUM_STRAGGLERS
fi

python $DIR/../wordcount.py \
    --redis-address $HEAD_IP:6379 \
    --words-file /home/ubuntu/ray/benchmarks/words.txt \
    --max-queue-length 4 \
    --batch-size $BATCH_SIZE \
    --num-mappers $NUM_RAYLETS \
    --num-reducers $NUM_RAYLETS \
    --num-records $NUM_RECORDS \
    --mapper-submit-batch-size $(( $NUM_RAYLETS / 2 )) \
    --target-throughput $THROUGHPUT \
    --latency-file $DIR/$latency_file $FAILURE_ARGS

bash $DIR/collect_latencies.sh $DIR/$latency_file
