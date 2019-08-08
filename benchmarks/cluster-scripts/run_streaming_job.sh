#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [[ $# -gt 5 || $# -lt 2 ]]
then
    echo "Usage: ./run_streaming_job.sh <head ip> <num raylets> <batch size> <target tput> <failure?> <num stragglers?>"
    exit
fi

HEAD_IP=$1
NUM_RAYLETS=$2
BATCH_SIZE=${3:-1000}
TOTAL_THROUGHPUT=${4:-$(( 12500 * $NUM_RAYLETS ))}
TEST_FAILURE=${5:-0}
NUM_SHARDS=8


latency_prefix=latency-$NUM_RAYLETS-workers-$NUM_SHARDS-shards-$BATCH_SIZE-batch-$TOTAL_THROUGHPUT-tput-
throughput_prefix=throughput-$NUM_RAYLETS-workers-$NUM_SHARDS-shards-$BATCH_SIZE-batch-$TOTAL_THROUGHPUT-tput-
DURATION=60

CHECKPOINT_DURATION=30
FAILURE_ARGS=""
if [[ $TEST_FAILURE -ne 0 ]]
then
    DURATION=$(( CHECKPOINT_DURATION * 5 / 2  ))
    FAILURE_ARGS="--num-mapper-failures 1 --fail-at $(( CHECKPOINT_DURATION * 3 / 2 ))"
    latency_prefix=failure-$latency_prefix$CHECKPOINT_DURATION-checkpoint-
    throughput_prefix=failure-$throughput_prefix$CHECKPOINT_DURATION-checkpoint-
fi

NUM_RECORDS=1000000
if [[ $TOTAL_THROUGHPUT -ne -1 ]]
then
    NUM_RECORDS=$(( $TOTAL_THROUGHPUT * $DURATION ))
fi

#if ls $latency_prefix* 1> /dev/null 2>&1
#then
#    echo "Latency file with prefix $latency_prefix already found, skipping..."
#    continue
#fi
date=`date +%h-%d-%M-%S`.txt
latency_file=$latency_prefix$date
throughput_file=$throughput_prefix$date
echo "Logging to file $latency_file..."


USE_GCS_ONLY=0
GCS_DELAY_MS=0
NONDETERMINISM=1
MAX_FAILURES=-1
OBJECT_STORE_MEMORY_GB=0
OBJECT_STORE_EVICTION=100

bash -x $DIR/start_cluster.sh \
    $NUM_RAYLETS \
    $NUM_SHARDS \
    $USE_GCS_ONLY \
    $GCS_DELAY_MS \
    $NONDETERMINISM \
    $MAX_FAILURES \
    $OBJECT_STORE_MEMORY_GB \
    $OBJECT_STORE_EVICTION

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
    --target-throughput $TOTAL_THROUGHPUT \
    --latency-file $DIR/$latency_file \
    --checkpoint-interval $(( $TOTAL_THROUGHPUT / $NUM_RAYLETS * $CHECKPOINT_DURATION )) \
    $FAILURE_ARGS

bash $DIR/collect_latencies.sh $DIR/$latency_file $DIR/$throughput_file
