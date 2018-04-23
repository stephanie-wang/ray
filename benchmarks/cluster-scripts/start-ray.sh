#!/bin/bash


export PATH=/home/ubuntu/anaconda3/bin/:$PATH

GCS_DELAY_MS="${1:-"-1"}"
HEAD=`head -n 1 workers.txt`
NUM_NODES="${NUM_NODES:-$(tail -n +2 workers.txt | wc -l)}"


rm /tmp/raylogs/*
if [ -z "$GCS_DELAY_MS" ]; then
    ray start --head --huge-pages --plasma-directory /mnt/hugepages --redis-port=6379 --use-raylet --use-task-shard
else
    ray start --head --huge-pages --plasma-directory /mnt/hugepages --redis-port=6379 --use-raylet --use-task-shard --gcs-delay-ms $GCS_DELAY_MS
fi

parallel-ssh -i -h <(tail -n +2 workers.txt) -x "-A -o StrictHostKeyChecking=no" -P "rm /tmp/raylogs/* || true"
sed -e 's/{HEAD}/'$HEAD'/g; s/{GCS_DELAY_MS}/'$GCS_DELAY_MS'/g' start-ray-node.sh.template > start-ray-node.sh
parallel-ssh -i -h <(tail -n $NUM_NODES workers.txt) -x "-A -o StrictHostKeyChecking=no" -P -I < start-ray-node.sh
