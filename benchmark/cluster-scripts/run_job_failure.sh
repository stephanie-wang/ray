NUM_RAYLETS=$1
NUM_REDIS_SHARDS=${2:-1}
NUM_REDUCERS=${3:-$NUM_RAYLETS}
EXPERIMENT_TIME=${4:-60}
GCS_DELAY_MS=${5:--1}
KILL_MAPPER=${6:-0}
WINDOW_SIZE=${7:-10}
USE_JSON=${8:-0}
OUTPUT_FILENAME=${9:-""}

USE_REDIS=1
THROUGHPUT=150000
NUM_PARSERS=1

LINEAGE_POLICY=1
MAX_LINEAGE_SIZE=1

HEAD_IP=$(head -n 1 workers.txt)

JSON_ARG=""
if [ $USE_JSON -eq 1 ]
then
    JSON_ARG="--use-json"
    #OUTPUT_FILENAME="$OUTPUT_FILENAME-json"
    NUM_PARSERS=4
fi

# Pick a node that isn't the reducer for now.
WORKER_RAYLETS=$(( $NUM_RAYLETS - 2 ))
if [ $KILL_MAPPER -eq 1 ]
then
    DEAD_NODE=$(( $RANDOM % $WORKER_RAYLETS + 2 ))
    DEAD_NODE_TYPE="mapper"
    #OUTPUT_FILENAME="$OUTPUT_FILENAME-map"
else
    DEAD_NODE=0
    DEAD_NODE_TYPE="reducer"
    #OUTPUT_FILENAME="$OUTPUT_FILENAME-reduce"
fi
echo "this job will kill a $DEAD_NODE_TYPE node, $DEAD_NODE"

if [ $# -gt 9 ] || [ $# -eq 0 ]
then
    echo "Usage: ./run_jobs.sh <num raylets> <num shards> <num reducers> <experiment time> <gcs delay> <use json> <kill mapper>"
    exit
else
    echo "Logging output to $OUTPUT_FILENAME"
fi


./stop_cluster.sh
./start_cluster.sh $NUM_RAYLETS $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS $NUM_REDIS_SHARDS

sleep 5

echo "Starting job..."

DUMP_ARG=""
if [ $EXPERIMENT_TIME -le 60 ]
then
    DUMP_ARG="--dump $OUTPUT_FILENAME.json"
fi

REDIS_ADDRESS=""
if [ $USE_REDIS -eq 1 ]
then
    echo "Starting redis for YSB results at $HEAD_IP:6380..."
    /home/ubuntu/redis-4.0.11/src/redis-cli -p 6380 shutdown
    REDIS_UP=PONG
    while [ ! -z $REDIS_UP ]; do
        REDIS_UP=$(/home/ubuntu/redis-4.0.11/src/redis-cli -p 6380 ping)
    done

    /home/ubuntu/redis-4.0.11/src/redis-server --port 6380 &
    REDIS_UP=""
    while [ -z $REDIS_UP ]; do
        REDIS_UP=$(/home/ubuntu/redis-4.0.11/src/redis-cli -p 6380 ping)
    done
    /home/ubuntu/redis-4.0.11/src/redis-cli -p 6380 CONFIG SET protected-mode no
    REDIS_ADDRESS="--reduce-redis-address $HEAD_IP:6380"
    echo "...Redis up"
fi


python ~/ray/benchmark/stream/ysb_stream_bench.py --redis-address $HEAD_IP --num-nodes $NUM_RAYLETS --num-parsers $NUM_PARSERS --target-throughput $THROUGHPUT --num-reducers $NUM_REDUCERS --exp-time $EXPERIMENT_TIME --num-reducers-per-node 2 $DUMP_ARG $REDIS_ADDRESS --output-filename $OUTPUT_FILENAME --actor-checkpointing $JSON_ARG --node-failure $DEAD_NODE --window-size $WINDOW_SIZE
