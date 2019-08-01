#!/bin/bash

NUM_RAYLETS=$1
REDIS_ADDRESS=$2
USE_GCS_ONLY=${3:-0}
GCS_DELAY_MS=${4:-0}
SHARD_SIZE=${5:-100}
BATCH_SIZE=${6:-64}
FAILURE=${7:-0}

NUM_REDIS_SHARDS=1

~/ray/benchmarks/cluster-scripts/start_cluster.sh $NUM_RAYLETS $NUM_REDIS_SHARDS $USE_GCS_ONLY $GCS_DELAY_MS


LOG_FILE=$NUM_RAYLETS"-workers-"$BATCH_SIZE"-batch-"$SHARD_SIZE"-shard-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-failure-`date +%y-%m-%d-%H-%M-%S`.out"
NUM_ITERATIONS=100
if [[ $FAILURE -eq 1 ]]; then
    LOG_FILE=failure-$LOG_FILE
    NUM_ITERATIONS=1500
    FAIL_AT=1200
    CHECKPOINT_INTERVAL=640
fi
echo "Logging to file $LOG_FILE"

cmd="python test_sgd_allreduce.py --redis-address $REDIS_ADDRESS:6379 --batch-size $BATCH_SIZE --num-workers $NUM_RAYLETS --devices-per-worker 4 --gpu --grad-shard-bytes "$SHARD_SIZE"000000 --num-iters $NUM_ITERATIONS --gcs-delay-ms $GCS_DELAY_MS"


if [ $USE_GCS_ONLY -eq 1 ]
then
    cmd=$cmd" --gcs-only"
fi

if [[ $FAILURE -eq 1 ]]
then
    cmd=$cmd" --checkpoint --stats-interval $CHECKPOINT_INTERVAL --test-failure --fail-at $FAIL_AT"
fi

echo "Running $cmd" | tee $LOG_FILE

$cmd 2>&1 | tee -a $LOG_FILE
