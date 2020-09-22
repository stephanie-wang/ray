#!/bin/bash


output="latency-`date`.csv"
echo "Writing output to $output"

for system in "leases" "centralized"; do
    yaml="benchmarks/latency-$system.yaml"
    ray up -y $yaml
    for placement in "--local" ""; do
        for use_actors in "--use-actors" ""; do
            for pipelined in "--pipelined" ""; do
                ray exec $yaml "source activate tensorflow_p36 && RAY_GCS_SERVICE_ENABLED=false python ray/benchmarks/latency.py --system $system --output '$output' $placement $use_actors $pipelined"
            done
        done
    done
    ray rsync_down $yaml "'/home/ubuntu/$output'" .
    mv "$output" "$system-$output"
done
