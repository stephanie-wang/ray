#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

HEAD_IP=$(head -n 1 ~/workers.txt)
NUM_RAYLETS=${1:-64}
OUTPUT_DIR=${2:-"$DIR/lineage-$(date +"%y-%m-%d-%H-%M-%S")"}
NUM_SHARDS=1

# Test with the lineage stash on, uncommitted lineage forwarding on, and a GCS
# delay of 100ms.
USE_GCS_ONLY=0
NONDETERMINISM=1
GCS_DELAY_MS=100

if [[ $# -ne 1 && $# -ne 2  && $# -ne 3 ]]
then
    echo "Usage: ./run_uncommitted_lineage_microbenchmark.sh <num raylets> <output dir>"
    exit
fi

echo "Creating output directory $OUTPUT_DIR..."
mkdir $OUTPUT_DIR

for TASK_DURATION in 000 001 002 003 004 005 006 007 008 009 010 020 030 040 050 100; do
    for MAX_FAILURES in 8 16 32 -1; do
        if [[ $MAX_FAILURES -lt $NUM_RAYLETS ]]
        then
            bash -x $DIR/run_microbenchmark_job.sh $NUM_RAYLETS $HEAD_IP $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $NUM_SHARDS $TASK_DURATION $MAX_FAILURES 0 $OUTPUT_DIR
        fi
    done
done
