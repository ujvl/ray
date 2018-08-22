NUM_RAYLETS=$1
LINEAGE_POLICY=$2
GCS_DELAY_MS=$3
NUM_REDIS_SHARDS=$4
THROUGHPUT=$5
OUT_FILENAME=$6

HEAD_IP=$(tail -n 1 workers.txt)
SENDING_WORKER_IP=$(head -n 1 workers.txt)
RECEIVING_WORKER_IP=$(head -n 2 workers.txt | tail -n 1)

if [ $# -eq 6 ] 
then
	echo "Running job with $NUM_RAYLETS raylets, lineage policy $LINEAGE_POLICY, GCS delay $GCS_DELAY_MS, throughput $THROUGHPUT, and $NUM_REDIS_SHARDS Redis shards..."
else
    echo "Usage: ./run_jobs.sh <num raylets> <lineage policy> <GCS delay> <num redis shards> <throughput> <out filename>"
    exit
fi


./stop_cluster.sh
./start_cluster.sh $NUM_RAYLETS $LINEAGE_POLICY $GCS_DELAY_MS $NUM_REDIS_SHARDS

python ~/ray/benchmark/actor_microbenchmark.py --target-throughput $THROUGHPUT --redis-address $HEAD_IP --num-raylets $NUM_RAYLETS 2>&1 | tee $OUT_FILENAME

ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $SENDING_WORKER_IP "tail /tmp/raylogs/raylet* | grep 'Lineage \|Queue'" >> $OUT_FILENAME
ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $RECEIVING_WORKER_IP "tail /tmp/raylogs/raylet* | grep 'Lineage \|Queue'" >> $OUT_FILENAME
