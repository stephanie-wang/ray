#!/bin/bash

HEAD_IP=$1
GCS_DELAY_MS=$2
NODE_RESOURCE=${3:-'Node'$RANDOM}
SLEEP_TIME=${4:-$(( $RANDOM % 5 ))}

SEND_THREADS=1
RECEIVE_THREADS=1


export PATH=/home/ubuntu/anaconda3/bin/:$PATH
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
# Necessary for deterministic partitioning to the reducers.
export PYTHONHASHSEED=0

ulimit -c unlimited
ulimit -n 65536
ulimit -a

echo "Sleeping for $SLEEP_TIME..."
sleep $SLEEP_TIME

ray start --redis-address=$HEAD_IP:6379 \
    --resources='{"'$NODE_RESOURCE'": 100}' \
    --object-store-memory 1000000000 \
    --num-cpus 4 \
    --internal-config='{
    "initial_reconstruction_timeout_milliseconds": 200,
    "gcs_delay_ms": '$GCS_DELAY_MS',
    "lineage_stash_max_failures": -1,
    "num_heartbeats_timeout": 10,
    "async_message_max_buffer_size": 100,
    "object_manager_repeated_push_delay_ms": 1000,
    "object_manager_pull_timeout_ms": 1000,
    "object_manager_send_threads": '$SEND_THREADS',
    "object_manager_receive_threads": '$RECEIVE_THREADS'}'

sleep 5

#i=0
#for pid in `pgrep -w raylet`; do
#    core=$(( i % 3  + 1 ))
#    taskset -pc $core $pid
#    sudo renice -n -10 -p $pid
#    i=$(( $i + 1 ))
#done

taskset -pc 0 `pgrep raylet`
sudo renice -n -19 -p `pgrep raylet`
#if [[ $# -ne 3 ]]; then
#    yes > /dev/null &
#    taskset -pc 0 $!
#fi

#for i in `seq 0 7`; do
#    yes > /dev/null &
#    taskset -pc $i $!
#done


#for pid in `pgrep -w python`; do
#    taskset -pc 1-3 $pid
#    #sudo renice -n -5 -p $pid
#done
#i=0
#for pid in `pgrep python`; do
#    echo $pid $i
#    taskset -pc $i $pid
#    i=$(($i + 1 ))
#done

sleep 1
