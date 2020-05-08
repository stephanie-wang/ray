#!/bin/bash


source activate tensorflow_p36
output="output-`date`.csv"
echo "Writing output to $output"

V07_ARG=$1

for delay in 10 50 100 500 1000; do
    python benchmarks/reconstruction.py $V07_ARG --delay-ms $delay --output "$output"
    python benchmarks/reconstruction.py $V07_ARG --delay-ms $delay --failure --output "$output"
    python benchmarks/reconstruction.py $V07_ARG --delay-ms $delay --large --output "$output"
    python benchmarks/reconstruction.py $V07_ARG --delay-ms $delay --failure --large --output "$output"
done
