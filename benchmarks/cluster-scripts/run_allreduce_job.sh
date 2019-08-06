#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
NUM_ITERATIONS=100

HEAD_IP=$1
NUM_RAYLETS=$2
OUTPUT_DIR=$3
NUM_FLOAT32=$4
USE_GCS_ONLY=$5
MAX_FAILURES=$6
GCS_DELAY_MS=$7
FAILURE=${8:-0}

NUM_SHARDS=1
NONDETERMINISM=0
MAX_FAILURES=1
OBJECT_STORE_MEMORY_GB=6
PEG=0
OBJECT_MANAGER_THREADS=4

if [[ $NUM_FLOAT32 -gt 25000000 ]]; then
  NUM_ITERATIONS=20
fi

latency_prefix=$OUTPUT_DIR"/latency-"$NUM_RAYLETS"-workers-"$NUM_SHARDS"-shards-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-"$NUM_FLOAT32"-bytes-"
FAILURE_ARG=""
if [[ $FAILURE -ne 0 ]]
then
    latency_prefix=failure-$latency_prefix
    #NUM_ITERATIONS=400
    #FAILURE_ARG="--test-failure --checkpoint-interval 150 --fail-at 280"
    NUM_ITERATIONS=200
    FAILURE_ARG="--test-failure --checkpoint-interval 50 --fail-at 90"
fi

if ls $latency_prefix* 1> /dev/null 2>&1
then
    echo "Latency file with prefix $latency_prefix already found, skipping..."
    exit
fi
latency_file=$latency_prefix`date +%y-%m-%d-%H-%M-%S`.txt

bash -x $DIR/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $MAX_FAILURES $OBJECT_STORE_MEMORY_GB $PEG

echo "Logging to file $latency_file..."
cmd="python $DIR/../allreduce.py --num-workers $NUM_RAYLETS --size $NUM_FLOAT32 --num-iterations $NUM_ITERATIONS --redis-address $HEAD_IP:6379 --object-store-memory-gb $OBJECT_STORE_MEMORY_GB --latency-file $latency_file $FAILURE_ARG"
echo $cmd | tee $latency_file
$cmd 2>&1 | tee -a $latency_file
