#!/bin/bash
set -e

num_nodes=$1
videos="$(printf '/home/ubuntu/husky.mp4 %.0s' {1..60})"

output="video-$num_nodes-nodes-`date`.csv"
echo "Writing output to $output"

args="--num-nodes 92 --video-path `printf '/home/ubuntu/husky.mp4 %.0s' {1..72}` --num-sinks 4 --num-sinks-per-node 2 --max 2400 --num-owners-per-node 4"

# Ownership.
yaml="benchmarks/cluster.yaml"
ray up -y $yaml; ray exec cluster.yaml "python ray/benchmarks/video_stabilizer.py $args --output 'ownership-$output'"
ray rsync_down $yaml "'/home/ubuntu/ownership-$output'" .
ray down -y $yaml

# Centralized.
yaml="benchmarks/ray_centralized.yaml"
ray up -y $yaml; ray exec cluster.yaml "python ray/benchmarks/video_stabilizer.py $args --output 'centralized-$output' --v07"
ray rsync_down $yaml "'/home/ubuntu/centralized-$output'" .
ray down -y $yaml
