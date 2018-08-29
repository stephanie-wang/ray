NUM_RAYLETS=$1
OUT_FILENAME=$2
COREDUMP=${3:-0}

WORKER_IPS=$(tail -n $(( $NUM_RAYLETS * 2 )) workers.txt)

echo "RECONSTRUCTIONS" >> $OUT_FILENAME
for WORKER in $WORKER_IPS; do
  ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $WORKER "grep 'Reconstruct' /tmp/raylogs/raylet* | wc -l" >> $OUT_FILENAME
  if [ $COREDUMP != "0" ]; then
      scp -i ~/devenv-key.pem $WORKER:~/core cores/core-$WORKER-$COREDUMP
  fi
done
