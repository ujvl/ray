#!/bin/bash

NUM_RAYLETS=$1
REDIS_ADDRESS=$2
USE_GCS_ONLY=${3:-0}
GCS_DELAY_MS=${4:-0}
SHARD_SIZE=${5:-100}
BATCH_SIZE=${6:-64}

NUM_REDIS_SHARDS=1

~/ray/benchmarks/cluster-scripts/start_cluster.sh $NUM_RAYLETS $NUM_REDIS_SHARDS $USE_GCS_ONLY $GCS_DELAY_MS

LOG_FILE=$NUM_RAYLETS"-workers-"$BATCH_SIZE"-batch-"$SHARD_SIZE"-shard-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-failure-`date +%y-%m-%d-%H-%M-%S`.out"
echo "Logging to file $LOG_FILE"

cmd="python test_sgd_allreduce.py --redis-address $REDIS_ADDRESS:6379 --batch-size $BATCH_SIZE --num-workers $NUM_RAYLETS --devices-per-worker 4 --gpu --grad-shard-bytes "$SHARD_SIZE"000000 --checkpoint --stats-interval 640 --num-iters 2000 --test-failure --fail-at 1200 --gcs-delay-ms $GCS_DELAY_MS"

if [ $USE_GCS_ONLY -eq 1 ]
then
    cmd=$cmd" --gcs-only"
fi

echo "Running $cmd" | tee $LOG_FILE

$cmd 2>&1 | tee -a $LOG_FILE
