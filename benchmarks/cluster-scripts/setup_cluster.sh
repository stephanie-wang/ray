#!/bin/bash


CLUSTER_YAML=$1
NUM_NODES=$2


ray up -y $CLUSTER_YAML


workers_file='workers.txt'
rm $workers_file

set -e

touch $workers_file

while [[ $( wc -l $workers_file | awk '{ print $1 }' ) -ne $(( $NUM_NODES + 1 )) ]];
do
    remaining_workers=$(( $NUM_NODES + 1 - $(wc -l $workers_file | awk '{ print $1 }') ))
    echo "Waiting for $remaining_workers more workers to start..."
    ray get-worker-ips $CLUSTER_YAML > workers.txt
    sleep 1
done
echo "All $NUM_NODES workers have started."

echo "Copying workers to head node..."
scp $workers_file ubuntu@`ray get_head_ip $CLUSTER_YAML`:~
rm $workers_file
echo "Done."

echo "Sleeping for 60s to give cluster time to come up..."
sleep 60
echo "Done"

echo "Initializing cluster..."
ray exec $CLUSTER_YAML "cd ~/ray && git remote set-url origin https://github.com/stephanie-wang/ray.git && git fetch origin && git checkout origin/lineage-stash"
ray exec $CLUSTER_YAML "~/ray/benchmarks/cluster-scripts/init.sh"
echo "Done"
