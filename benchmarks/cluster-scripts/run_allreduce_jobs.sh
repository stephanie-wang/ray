#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

HEAD_IP=$(head -n 1 ~/workers.txt)
NUM_NODES=${1:-64}
OUTPUT_DIR=${2:-"$DIR/allreduce-$(date +"%y-%m-%d-%H-%M-%S")"}

if [[ $# -lt 1 || $# -gt 2 ]]
then
    echo "Usage: ./run_allreduce_jobs.sh <num raylets> <output dir>"
    exit
fi

echo "Creating output directory $OUTPUT_DIR..."
mkdir $OUTPUT_DIR

# Make sure hugepages is set to 0.
parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-oStrictHostKeyChecking=no -i /home/ubuntu/ray_bootstrap_key.pem" -I 'bash -s - '$NR_HUGEPAGES < $DIR/enable_hugepages.sh
# Run the failure experiment for OpenMPI.
pushd .
cd ~/mpi-bench
rm ~/mpi-bench/*txt
bash -x failure-bench.sh $NUM_NODES
popd
# Run the failure experiment for lineage stash with and without the GCS.
NUM_FLOAT32=25000000
GCS_DELAY_MS=0
MAX_FAILURES=1
CHECKPOINT_INTERVAL=150
bash -x $DIR/run_allreduce_job.sh $HEAD_IP $NUM_NODES $OUTPUT_DIR $NUM_FLOAT32 0 $MAX_FAILURES $GCS_DELAY_MS $CHECKPOINT_INTERVAL
bash -x $DIR/run_allreduce_job.sh $HEAD_IP $NUM_NODES $OUTPUT_DIR $NUM_FLOAT32 1 $MAX_FAILURES $GCS_DELAY_MS $CHECKPOINT_INTERVAL

# Make sure hugepages is set to 0.
parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-oStrictHostKeyChecking=no -i /home/ubuntu/ray_bootstrap_key.pem" -I 'bash -s - '$NR_HUGEPAGES < $DIR/enable_hugepages.sh
# Run latency experiments without failure.
pushd .
cd ~/mpi-bench
bash -x bench.sh $NUM_NODES
popd
for NUM_FLOAT32 in 2500000 25000000 250000000; do
    for GCS_DELAY_MS in 0 5; do
        USE_GCS=0
        MAX_FAILURES=1
        bash -x $DIR/run_allreduce_job.sh $HEAD_IP $NUM_NODES $OUTPUT_DIR $NUM_FLOAT32 $USE_GCS $MAX_FAILURES $GCS_DELAY_MS

        USE_GCS=1
        MAX_FAILURES=0
        bash -x $DIR/run_allreduce_job.sh $HEAD_IP $NUM_NODES $OUTPUT_DIR $NUM_FLOAT32 $USE_GCS $MAX_FAILURES $GCS_DELAY_MS
    done
done

# Move the OpenMPI outputs to the same result directory.
mv ~/mpi-bench/*txt $OUTPUT_DIR

echo "Done! All outputs in $OUTPUT_DIR"
