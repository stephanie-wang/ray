#!/bin/bash

NUM_RAYLETS=$1
HEAD_IP=$2
USE_GCS_ONLY=$3
GCS_DELAY_MS=$4
NONDETERMINISM=$5
NUM_SHARDS=$6
TASK_DURATION=$7
MAX_FAILURES=$8
COLLECT_LATENCY=$9
OUTPUT_DIR=${10:-"$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"}

HOME_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )

NUM_TASKS=100
NUM_ITERATIONS=1

if [[ $# -eq 9 || $# -eq 10 ]]
then
    echo "Running with $NUM_RAYLETS workers, use gcs only? $USE_GCS_ONLY, $GCS_DELAY_MS ms GCS delay, nondeterminism? $NONDETERMINISM"
else
    echo "Usage: ./run_job.sh <num raylets> <head IP address> <use gcs only> <GCS delay ms> <nondeterminism> <num shards> <task duration> <num failures> <collect latency?> <output dir>"
    exit
fi

if [[ $COLLECT_LATENCY -eq 1 ]]
then
    output_prefix=$OUTPUT_DIR"/latency-"$NUM_RAYLETS"-workers-"$NUM_SHARDS"-shards-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-"$NONDETERMINISM"-nondeterminism-"$TASK_DURATION"-task-"$MAX_FAILURES"-failures-"
else
    output_prefix=$OUTPUT_DIR"/lineage-"$NUM_RAYLETS"-workers-"$NUM_SHARDS"-shards-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-"$NONDETERMINISM"-nondeterminism-"$TASK_DURATION"-task-"$MAX_FAILURES"-failures-"
fi

if ls $output_prefix* 1> /dev/null 2>&1
then
    echo "File with prefix $output_prefix already found, skipping..."
    exit
fi

output_prefix=$output_prefix`date +%y-%m-%d-%H-%M-%S`
output_file=$output_prefix.csv

exit_code=1
i=0
while [[ $exit_code -ne 0 ]]
do
    echo "Attempt #$i"
    if [[ $i -eq 3 ]]
    then
        echo "Failed 3 attempts " >> $output_file
        exit
    fi

    bash -x $HOME_DIR/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $MAX_FAILURES

    echo "Logging to file $output_file..."
    cmd="python $HOME_DIR/../ring_microbenchmark.py --num-workers $NUM_RAYLETS --num-tasks $NUM_TASKS --num-iterations $NUM_ITERATIONS --redis-address $HEAD_IP:6379 --gcs-delay $GCS_DELAY_MS --latency-file $output_file --task-duration 0."$TASK_DURATION

    if [[ $NONDETERMINISM -eq 1 ]]
    then
      cmd=$cmd" --nondeterminism"
    fi
    if [[ $USE_GCS_ONLY -eq 1 ]]
    then
      cmd=$cmd" --gcs-only"
    fi

    echo $cmd
    $cmd 2>&1

    exit_code=${PIPESTATUS[0]}

    i=$(( $i + 1 ))
done

echo "Collecting stats from workers..."
if [[ $COLLECT_LATENCY -eq 1 ]]
then
    echo "latency" >> $output_file
    for worker in `tail -n $NUM_RAYLETS ~/workers.txt`; do
        ssh -o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem $worker "grep LATENCY /tmp/ray/*/logs/worker*" | awk -F'LATENCY:' '{ print (($2 / '$NUM_RAYLETS') - '$TASK_DURATION') * 1000 }' >> $output_file
    done
else
    echo "worker,timestamp,num_tasks,uncommitted_lineage" >> $output_file
    for worker in `tail -n $NUM_RAYLETS ~/workers.txt`; do
      ssh -o "StrictHostKeyChecking no" -i ~/ray_bootstrap_key.pem $worker "grep 'UNCOMMITTED' /tmp/ray/*/logs/raylet.err" | awk -F':' '{ print '$worker'","$5 }' | tail -n $NUM_RAYLETS >> $output_file
    done
fi
