HEAD_IP=$1
WORKER_INDEX=$2
NUM_RAYLETS=$3
LINEAGE_POLICY=$4
MAX_LINEAGE_SIZE=$5
GCS_DELAY_MS=$6

export PATH=/home/ubuntu/anaconda3/bin/:$PATH

for i in `seq 0 $(($NUM_RAYLETS - 1))`; do
    ray start --num-workers 0 --use-raylet --redis-address=$HEAD_IP:6379 --resources='{"Node'$(($i * 2 + $WORKER_INDEX))'": 1}' --gcs-delay-ms $GCS_DELAY_MS --lineage-cache-policy=$LINEAGE_POLICY --max-lineage-size=$MAX_LINEAGE_SIZE
done
