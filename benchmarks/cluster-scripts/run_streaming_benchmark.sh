#!/bin/bash

head_node_ip=$(head -n 1 ~/workers.txt)
NUM_WORKERS=$1

# For m4.xlarge, this should be 8000.
SOURCE_RATE_PER_WORKER=$(( 12500 * 3 / 4))
# Run the Flink failure  experiment.
PDSH_RCMD_TYPE=ssh bash ~/flink-wordcount/run_job.sh $head_node_ip $NUM_WORKERS $(( $SOURCE_RATE_PER_WORKER * $NUM_WORKERS )) 1
# Run the lineage stash failure experiment.
bash ~/ray/benchmarks/cluster-scripts/run_streaming_job.sh $head_node_ip $NUM_WORKERS $(( $SOURCE_RATE_PER_WORKER * $NUM_WORKERS )) 1
# Run the WriteFirst failure experiment.
bash ~/ray/benchmarks/cluster-scripts/run_streaming_job.sh $head_node_ip $NUM_WORKERS $(( $SOURCE_RATE_PER_WORKER * $NUM_WORKERS )) 1 0

SOURCE_RATE_PER_WORKER=12500
# Run the Flink latency experiment.
PDSH_RCMD_TYPE=ssh bash ~/flink-wordcount/run_job.sh $head_node_ip $NUM_WORKERS $(( $SOURCE_RATE_PER_WORKER * $NUM_WORKERS ))
# Run the lineage stash latency experiment.
bash ~/ray/benchmarks/cluster-scripts/run_streaming_job.sh $head_node_ip $NUM_WORKERS $(( $SOURCE_RATE_PER_WORKER * $NUM_WORKERS ))
# Run the WriteFirst latency experiment.
bash ~/ray/benchmarks/cluster-scripts/run_streaming_job.sh $head_node_ip $NUM_WORKERS $(( $SOURCE_RATE_PER_WORKER * $NUM_WORKERS )) 0 0
