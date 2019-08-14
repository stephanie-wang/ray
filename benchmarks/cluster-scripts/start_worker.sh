#!/bin/bash

HEAD_IP=$1
USE_GCS_ONLY=$2
GCS_DELAY_MS=$3
NONDETERMINISM=$4
MAX_FAILURES=$5
OBJECT_STORE_MEMORY_GB=$6
OBJECT_STORE_EVICTION=$7
PEG=$8
OBJECT_MANAGER_THREADS=$9
NODE_RESOURCE=${10:-'Node'$RANDOM}
SLEEP_TIME=${11:-$(( $RANDOM % 5 ))}

#source activate tensorflow_p36
#export PATH=/home/ubuntu/anaconda3/envs/tensorflow_p36/bin/:$PATH
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

if [[ $NONDETERMINISM -eq 1 && $MAX_FAILURES -eq 1 ]]
then
  MAX_FAILURES=-1
fi

hugepages_config=''
if [[ $OBJECT_STORE_MEMORY_GB -gt 0 ]]
then
    # Turn on hugepages. Set object store capacity to be the number of
    # allocated hugepages * 2MB/hugepage.
    object_store_memory=$(( $OBJECT_STORE_MEMORY_GB * (10 ** 9) ))
    hugepages_config='
      --plasma-directory=/mnt/hugepages
      --huge-pages
      --object-store-memory '$object_store_memory
    echo "Starting object store with hugepages enabled, $object_store_memory bytes"
fi

ray start --redis-address=$HEAD_IP:6379 \
    --resources='{"'$NODE_RESOURCE'": 100}' \
    --plasma-eviction-fraction $OBJECT_STORE_EVICTION \
    $hugepages_config \
    --num-cpus 4 \
    --internal-config='{
    "initial_reconstruction_timeout_milliseconds": 200,
    "use_gcs_only": '$USE_GCS_ONLY',
    "gcs_delay_ms": '$GCS_DELAY_MS',
    "lineage_stash_max_failures": '$MAX_FAILURES',
    "log_nondeterminism": '$NONDETERMINISM',
    "num_heartbeats_timeout": 20,
    "async_message_max_buffer_size": 100,
    "object_manager_repeated_push_delay_ms": 1000,
    "object_manager_pull_timeout_ms": 1000,
    "object_manager_send_threads": '$OBJECT_MANAGER_THREADS',
    "object_manager_receive_threads": '$OBJECT_MANAGER_THREADS'}'

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
if [[ $PEG -ne 0 ]]; then
    yes > /dev/null &
    taskset -pc 0 $!
fi

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
