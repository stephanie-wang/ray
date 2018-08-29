LINEAGE_POLICY=$1
MAX_LINEAGE_SIZE=$2
GCS_DELAY_MS=$3
NUM_REDIS_SHARDS=$4
LEASE_FACTOR=$5

export PATH=/home/ubuntu/anaconda3/bin/:$PATH

ray start --num-workers 0 --use-raylet --head --redis-port=6379 --gcs-delay-ms $GCS_DELAY_MS --num-redis-shards $NUM_REDIS_SHARDS --lineage-cache-policy=$LINEAGE_POLICY --max-lineage-size=$MAX_LINEAGE_SIZE --lease-factor=$LEASE_FACTOR
