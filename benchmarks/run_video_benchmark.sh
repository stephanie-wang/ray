#!/bin/bash
set -e

num_nodes=$1
half=$((num_nodes / 2))
videos="$(printf '/home/ubuntu/husky.mp4 %.0s' `seq 1 $half`) $(printf '/home/ubuntu/pitbull.mkv %.0s' `seq 1 $half`)"

output="video-$num_nodes-nodes-`date`.csv"
echo "Writing output to $output"

# Ownership.
yaml="benchmarks/cluster.yaml"
ray up -y $yaml
sleep 5
# Warmup.
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer.py --num-nodes $num_nodes --video-path $videos"
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer.py --num-nodes $num_nodes --video-path $videos"
# ~60s of video.
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer.py --num-nodes $num_nodes --video-path $videos --max-frames 1800 --output 'ownership-$output'"
# With failure.
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer.py --num-nodes $num_nodes --video-path $videos --max-frames 1800 --failure --output 'ownership-failure-$output'"
ray rsync_down $yaml "'/home/ubuntu/ownership-$output'" .
ray rsync_down $yaml "'/home/ubuntu/ownership-failure-$output'" .

# Leases.
yaml="benchmarks/ray_0_7.yaml"
ray up -y $yaml
sleep 5
# Warmup.
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer_0_7.py --num-nodes $num_nodes --video-path $videos"
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer_0_7.py --num-nodes $num_nodes --video-path $videos"
# ~60s of video.
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer_0_7.py --num-nodes $num_nodes --video-path $videos --max-frames 1800 --output 'leases-$output'"
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer_0_7.py --num-nodes $num_nodes --video-path $videos --max-frames 1800 --failure --output 'leases-failure-$output'"
ray rsync_down $yaml "'/home/ubuntu/leases-$output'" .
ray rsync_down $yaml "'/home/ubuntu/leases-failure-$output'" .

# Centralized
yaml="benchmarks/ray_centralized.yaml"
ray up -y $yaml
sleep 5
# Warmup.
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer_0_7.py --num-nodes $num_nodes --video-path $videos"
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer_0_7.py --num-nodes $num_nodes --video-path $videos"
# ~60s of video.
ray exec $yaml "source activate tensorflow_p36 && python ray/benchmarks/video_stabilizer_0_7.py --num-nodes $num_nodes --video-path $videos --max-frames 1800 --output 'centralized-$output'"
ray rsync_down $yaml "'/home/ubuntu/centralized-$output'" .
