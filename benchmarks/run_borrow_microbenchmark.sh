#!/bin/bash


output="borrow-`date`.csv"
echo "Writing output to $output"

#source activate ray-0-7
#for system in "leases" "centralized"; do
#    yaml="borrowing-$system.yaml"
#    ray up -y $yaml
#    for borrowers in 0 1 2 4 8 16; do
#        echo "$borrowers borrowers"
#        ray exec $yaml "python ray/benchmarks/borrowing.py --system $system --num-borrowers $borrowers --output '$output'"
#    done
#    ray rsync_down $yaml "'/home/ubuntu/$output'" .
#    mv "$output" "$system-$output"
#done

conda deactivate
source activate ray-wheel-36
system="ownership"
yaml="borrowing-$system.yaml"
ray up -y $yaml
for borrowers in 0 1 2 4 8 16; do
    echo "$borrowers borrowers"
    ray exec $yaml "python ray/benchmarks/borrowing.py --system $system --num-borrowers $borrowers --output '$output'"
done
ray rsync_down $yaml "'/home/ubuntu/$output'" .
mv "$output" "$system-$output"
