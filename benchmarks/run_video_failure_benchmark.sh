#!/bin/bash
set -e

num_nodes=$1
output="video-$num_nodes-nodes-`date`"
echo "Writing output to $output"

args="--num-nodes 77 --video-path `printf '/home/ubuntu/husky.mp4 %.0s' {1..60}` --num-sinks 4 --num-sinks-per-node 2 --max 2400 --num-owners-per-node 4"

# Ownership.
yaml="benchmarks/cluster.yaml"
ray up -y $yaml; ray exec cluster.yaml "mkdir 'ownership-$output'"
ray up -y $yaml; ray exec cluster.yaml "python ray/benchmarks/video_stabilizer.py $args --output 'ownership-$output/ownership-checkpoint.csv' --checkpoint 30"
ray up -y $yaml; ray exec cluster.yaml "python ray/benchmarks/video_stabilizer.py $args --output 'ownership-$output/ownership-video.csv'"
ray up -y $yaml; ray exec cluster.yaml "python ray/benchmarks/video_stabilizer.py $args --output 'ownership-$output/ownership-worker-failure.csv' --failure"
ray up -y $yaml; ray exec cluster.yaml "python ray/benchmarks/video_stabilizer.py $args --output 'ownership-$output/ownership-owner-failure.csv' --owner-failure"
ray up -y $yaml; ray exec cluster.yaml "python ray/benchmarks/video_stabilizer.py $args --output 'ownership-$output/ownership-owner-checkpoint-failure.csv' --owner-failure --checkpoint-interval 30"
ray rsync_down $yaml "'/home/ubuntu/ownership-$output'" .
ray down -y $yaml

# Leases.
yaml="benchmarks/ray_0_7.yaml"
ray up -y $yaml; ray exec cluster.yaml "mkdir 'leases-$output'"
ray up -y $yaml; ray exec $yaml "python ray/benchmarks/video_stabilizer.py $args --v07 --output 'leases-$output/leases-video.csv'"
ray up -y $yaml; ray exec $yaml "python ray/benchmarks/video_stabilizer.py $args --v07 --output 'leases-$output/leases-failure.csv' --failure"
ray rsync_down $yaml "'/home/ubuntu/leases-$output'" .
ray down -y $yaml
