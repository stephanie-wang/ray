NUM_RAYLETS=$1
THROUGHPUT=${2:-100000}
NUM_REDIS_SHARDS=${3:-1}
EXPERIMENT_TIME=$3

LINEAGE_POLICY=0
MAX_LINEAGE_SIZE=100
GCS_DELAY_MS=-1

HEAD_IP=$(head -n 1 workers.txt)

if [ $# -eq 1 ]
then
	echo "Running job with $NUM_RAYLETS raylets, lineage policy $LINEAGE_POLICY, GCS delay $GCS_DELAY_MS, and $NUM_REDIS_SHARDS Redis shards..."
elif [ $# -eq 2 ]
then
	echo "Running job with $NUM_RAYLETS raylets, lineage policy $LINEAGE_POLICY, GCS delay $GCS_DELAY_MS, throughput $THROUGHPUT, and $NUM_REDIS_SHARDS Redis shards..."
elif [ $# -eq 3 ]
then
	echo "Running job with $NUM_RAYLETS raylets, lineage policy $LINEAGE_POLICY, GCS delay $GCS_DELAY_MS, throughput $THROUGHPUT, and $NUM_REDIS_SHARDS Redis shards..."
else
    echo "Usage: ./run_jobs.sh <num raylets> <throughput> <experiment time>"
    exit
fi


./stop_cluster.sh
./start_cluster.sh $NUM_RAYLETS $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS $NUM_REDIS_SHARDS

sleep 5

echo "Starting job..."

python ~/ray/benchmark/stream/ysb_stream_bench.py --redis-address $HEAD_IP --num-nodes $NUM_RAYLETS --num-parsers 2 --target-throughput $THROUGHPUT
