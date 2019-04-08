#!/bin/bash

NUM_ITERATIONS=100

NUM_RAYLETS=$1
HEAD_IP=$2
SIZE=$3
USE_GCS_ONLY=$4
GCS_DELAY_MS=$5
NUM_SHARDS=${6:-1}

FAILURE=${7:-0}
FAILURE_ARG=""

latency_prefix="latency-"$NUM_RAYLETS"-workers-"$NUM_SHARDS"-shards-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-"$SIZE"-bytes-"
if [[ $FAILURE -ne 0 ]]
then
    latency_prefix=failure-$latency_prefix
    NUM_ITERATIONS=400
    FAILURE_ARG="--test-failure --checkpoint-interval 150 --fail-at 280"
fi

if ls $latency_prefix* 1> /dev/null 2>&1
then
    echo "Latency file with prefix $latency_prefix already found, skipping..."
    continue
fi
latency_file=$latency_prefix`date +%y-%m-%d-%H-%M-%S`.txt

bash -x ./cluster-scripts/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS $USE_GCS_ONLY $GCS_DELAY_MS

echo "Logging to file $latency_file..."
cmd="python allreduce.py --num-workers $NUM_RAYLETS --size $SIZE --num-iterations $NUM_ITERATIONS --redis-address $HEAD_IP:6379 --latency-file $latency_file $FAILURE_ARG"
echo $cmd | tee $latency_file
$cmd 2>&1 | tee -a $latency_file
