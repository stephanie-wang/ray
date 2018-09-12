NUM_RAYLETS=$1
NUM_REDIS_SHARDS=${2:-1}
NUM_REDUCERS=${3:-$NUM_RAYLETS}
EXPERIMENT_TIME=${4:-60}
GCS_DELAY_MS=${5:--1}

THROUGHPUT=150000

LINEAGE_POLICY=1
MAX_LINEAGE_SIZE=1

HEAD_IP=$(head -n 1 workers.txt)

if [ $# -gt 5 ] || [ $# -eq 0 ]
then
    echo "Usage: ./run_jobs.sh <num raylets> <num shards> <num reducers> <experiment time> <gcs delay>"
    exit
fi


./stop_cluster.sh
./start_cluster.sh $NUM_RAYLETS $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS $NUM_REDIS_SHARDS

sleep 5

echo "Starting job..."

DUMP_ARG=""
if [ $EXPERIMENT_TIME -le 60 ]
then
    DUMP_ARG="--dump dump.json"
fi

python ~/ray/benchmark/stream/ysb_stream_bench.py --redis-address $HEAD_IP --num-nodes $NUM_RAYLETS --num-parsers 2 --target-throughput $THROUGHPUT --actor-checkpointing --num-reducers $NUM_REDUCERS --exp-time $EXPERIMENT_TIME --num-reducers-per-node 4 $DUMP_ARG
