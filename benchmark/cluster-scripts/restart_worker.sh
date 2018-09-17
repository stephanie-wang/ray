HEAD_IP=$1
WORKER_INDEX=$2
LINEAGE_POLICY=1
MAX_LINEAGE_SIZE=1
GCS_DELAY_MS=-1


export PATH=/home/ubuntu/anaconda3/bin/:$PATH

ray stop

ulimit -n 65536

ray start --num-workers 12 --use-raylet --redis-address=$HEAD_IP:6379 --resources='{"Node'$WORKER_INDEX'": 100}' --gcs-delay-ms $GCS_DELAY_MS --lineage-cache-policy=$LINEAGE_POLICY --max-lineage-size=$MAX_LINEAGE_SIZE --plasma-directory=/mnt/hugepages --huge-pages --num-cpus 4 --object-store-memory 10000000000
