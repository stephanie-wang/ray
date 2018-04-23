#!/bin/bash

export PATH=/home/ubuntu/anaconda3/bin/:$PATH

NUM_NODES=$1
NUM_REDIS_SHARDS=$2
GCS_DELAY_MS="${3:-"-1"}"

./start-ray.sh $NUM_NODES $NUM_REDIS_SHARDS $GCS_DELAY_MS

pushd .

cd ~/ray/benchmarks
python tree.py --num-recursions 12 --redis-address 172.30.0.164:6379 --use-raylet 2>&1 | tee "xray-$GCS_DELAY_MS-gcs-delay-$NUM_NODES-nodes-$NUM_REDIS_SHARDS-shards.out"

popd

./stop-ray.sh
