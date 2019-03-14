#!/bin/bash

NUM_ITERATIONS=100

NUM_RAYLETS=$1
SIZE=$2
GCS_DELAY_MS=$3
NUM_SHARDS=$4
HEAD_IP=$5

FAILURE=${6:-0}
FAILURE_ARG=""
if [[ $FAILURE -ne 0 ]]
then
    latency_prefix=failure-$latency_prefix
    FAILURE_ARG="--test-failure"
fi

latency_prefix=latency-$NUM_RAYLETS-workers-$NUM_SHARDS-shards-$GCS_DELAY_MS-gcs-$SIZE-bytes-

if ls $latency_prefix* 1> /dev/null 2>&1
then
    echo "Latency file with prefix $latency_prefix already found, skipping..."
    continue
fi
latency_file=$latency_prefix`date +%h-%d-%M-%S`.txt
echo "Logging to file $latency_file..."

bash -x ./cluster-scripts/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS $GCS_DELAY_MS
python allreduce.py --num-workers $NUM_RAYLETS --size $SIZE --num-iterations $NUM_ITERATIONS --redis-address $HEAD_IP:6379 --latency-file $latency_file $FAILURE_ARG
