LINEAGE_POLICY=$1
GCS_DELAY_MS=$2
NUM_REDIS_SHARDS=$3

export PATH=/home/ubuntu/anaconda3/bin/:$PATH

ray start --num-workers 0 --use-raylet --head --redis-port=6379 --gcs-delay-ms $GCS_DELAY_MS --num-redis-shards $NUM_REDIS_SHARDS --lineage-cache-policy=$LINEAGE_POLICY
