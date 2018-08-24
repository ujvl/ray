NUM_RAYLETS=$1
OUT_FILENAME=$2

WORKER_IPS=$(tail -n $(( $NUM_RAYLETS * 2 )) workers.txt)

for WORKER in $WORKER_IPS; do
  ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $WORKER "tail /tmp/raylogs/raylet* | grep 'Lineage \|Queue'" >> $OUT_FILENAME
done
