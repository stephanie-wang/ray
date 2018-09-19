NUM_RAYLETS=$1
LINEAGE_POLICY=$2
MAX_LINEAGE_SIZE=$3
GCS_DELAY_MS=$4
NUM_REDIS_SHARDS=$5

HEAD_IP=$(head -n 1 workers.txt)
WORKER_IPS=$(tail -n $(( $NUM_RAYLETS * 2 )) workers.txt)

if [ $# -eq 5 ]
then
	echo "Starting cluster with $NUM_RAYLETS raylets, GCS delay $GCS_DELAY_MS, and $NUM_REDIS_SHARDS Redis shards..."
else
    echo "Usage: ./start_cluster.sh <num raylets> <lineage policy> <max lineage size> <GCS delay> <num redis shards>"
    exit
fi

echo "Starting head with GCS delay $GCS_DELAY_MS and $NUM_REDIS_SHARDS Redis shards..."
./start_head.sh $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS $NUM_REDIS_SHARDS
echo "Done starting head"

echo "Starting workers $WORKER_IPS with GCS delay $GCS_DELAY_MS and $NUM_RAYLETS raylets..."
parallel-ssh -t 0 -i -P -H "$WORKER_IPS" -O "StrictHostKeyChecking=no" -O "IdentityFile=~/devenv-key.pem" -I 'bash -s - '$HEAD_IP '$PSSH_NODENUM' $NUM_RAYLETS $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS < start_worker.sh
echo "Done starting workers"

sleep 5
parallel-ssh -t 0 -i -P -H "$WORKER_IPS" -O "StrictHostKeyChecking=no" -O "IdentityFile=~/devenv-key.pem" 'taskset -p -c 0,1 `pgrep raylet`'
