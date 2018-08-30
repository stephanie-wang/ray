NUM_RAYLETS=$1
LINEAGE_POLICY=$2
MAX_LINEAGE_SIZE=$3
GCS_DELAY_MS=$4
NUM_REDIS_SHARDS=$5
THROUGHPUT=$6
OUT_FILENAME=$7
SAMPLE=$8
GROUP_SIZE=${9:-1}

HEAD_IP=$(head -n 1 workers.txt)
WORKER_IPS=$(tail -n $(( $NUM_RAYLETS * 2 )) workers.txt)

if [ $# -eq 8 ]
then
	echo "Running job with $NUM_RAYLETS raylets, lineage policy $LINEAGE_POLICY, GCS delay $GCS_DELAY_MS, throughput $THROUGHPUT, and $NUM_REDIS_SHARDS Redis shards..."
elif [ $# -eq 9 ]
then
	echo "Running job with $NUM_RAYLETS raylets, lineage policy $LINEAGE_POLICY, GCS delay $GCS_DELAY_MS, throughput $THROUGHPUT, and $NUM_REDIS_SHARDS Redis shards, $GROUP_SIZE group size..."
else
    echo "Usage: ./run_jobs.sh <num raylets> <lineage policy> <max lineage size> <GCS delay> <num redis shards> <throughput> <out filename>"
    exit
fi


./stop_cluster.sh
./start_cluster.sh $NUM_RAYLETS $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS $NUM_REDIS_SHARDS

sleep 5

echo "Starting job..."
GCS_ARG=""
if [ $GCS_DELAY_MS = 0 ]; then
    GCS_ARG="--gcs"
fi

SAMPLE_ARG=""
if [ $SAMPLE = "local" ] || [ $SAMPLE = "remote" ]; then
    SAMPLE_ARG="--sample-$SAMPLE"
fi

python ~/ray/benchmark/latency_microbenchmark.py --redis-address $HEAD_IP --num-raylets $NUM_RAYLETS --group-size $GROUP_SIZE --target-throughput $THROUGHPUT --num-shards $NUM_REDIS_SHARDS $GCS_ARG $SAMPLE_ARG 2>&1 | tee -a $OUT_FILENAME
