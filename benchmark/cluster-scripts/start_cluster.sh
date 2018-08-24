NUM_RAYLETS=$1
LINEAGE_POLICY=$2
MAX_LINEAGE_SIZE=$3
GCS_DELAY_MS=$4
NUM_REDIS_SHARDS=$5

HEAD_IP=$(tail -n 1 workers.txt)
SENDING_WORKER_IP=$(head -n 1 workers.txt)
RECEIVING_WORKER_IP=$(head -n 2 workers.txt | tail -n 1)

if [ $# -eq 5 ]
then
	echo "Starting cluster with $NUM_RAYLETS raylets, GCS delay $GCS_DELAY_MS, and $NUM_REDIS_SHARDS Redis shards..."
else
    echo "Usage: ./start_cluster.sh <num raylets> <lineage policy> <max lineage size> <GCS delay> <num redis shards>"
    exit
fi

echo "Starting head with GCS delay $GCS_DELAY_MS and $NUM_REDIS_SHARDS Redis shards..."
ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $HEAD_IP bash -s - < start_head.sh $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS $NUM_REDIS_SHARDS
echo "Done starting head"

echo "Starting sending worker $SENDING_WORKER_IP with GCS delay $GCS_DELAY_MS and $NUM_RAYLETS raylets..."
ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $SENDING_WORKER_IP bash -s - < start_worker.sh $HEAD_IP 0 $NUM_RAYLETS $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS
echo "Done starting sending worker"

echo "Starting receiving worker $RECEIVING_WORKER_IP with GCS delay $GCS_DELAY_MS and $NUM_RAYLETS raylets..."
ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $RECEIVING_WORKER_IP bash -s - < start_worker.sh $HEAD_IP 1 $NUM_RAYLETS $LINEAGE_POLICY $MAX_LINEAGE_SIZE $GCS_DELAY_MS
echo "Done starting receiving worker"
