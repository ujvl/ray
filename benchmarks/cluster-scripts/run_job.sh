#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

NUM_RAYLETS=$1
HEAD_IP=$2
USE_GCS_ONLY=$3
GCS_DELAY_MS=$4
NONDETERMINISM=$5
NUM_SHARDS=$6
TASK_DURATION=${7:-0}

NUM_TASKS=10
NUM_ITERATIONS=1

if [[ $# -eq 7 || $# -eq 6 ]]
then
    echo "Running with $NUM_RAYLETS workers, use gcs only? $USE_GCS_ONLY, $GCS_DELAY_MS ms GCS delay, nondeterminism? $NONDETERMINISM"
else
    echo "Usage: ./run_job.sh <num raylets> <head IP address> <use gcs only> <GCS delay ms> <nondeterminism> <num shards>"
    exit
fi
latency_prefix=$DIR"/latency-"$NUM_RAYLETS"-workers-"$NUM_SHARDS"-shards-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-"$NONDETERMINISM"-nondeterminism-"$TASK_DURATION"-task-"

if ls $latency_prefix* 1> /dev/null 2>&1
then
    echo "Latency file with prefix $latency_prefix already found, skipping..."
    exit
fi

latency_prefix=$latency_prefix`date +%y-%m-%d-%H-%M-%S`
latency_file=$latency_prefix.txt
raylet_log_file=$latency_prefix.out

exit_code=1
i=0
while [[ $exit_code -ne 0 ]]
do
    if [[ $i -eq 3 ]]
    then
        echo "Failed 3 attempts " >> $latency_file
    fi

    bash -x $DIR/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM

    echo "Logging to file $latency_file..."
    cmd="python $DIR/../ring_microbenchmark.py --num-workers $NUM_RAYLETS --num-tasks $NUM_TASKS --num-iterations $NUM_ITERATIONS --redis-address $HEAD_IP:6379 --gcs-delay $GCS_DELAY_MS --latency-file $latency_file --task-duration 0."$TASK_DURATION

    if [[ $NONDETERMINISM -eq 1 ]]
    then
      cmd=$cmd" --nondeterminism"
    fi
    if [[ $USE_GCS_ONLY -eq 1 ]]
    then
      cmd=$cmd" --gcs-only"
    fi

    echo $cmd | tee -a $latency_file
    $cmd 2>&1 | tee -a $latency_file

    exit_code=${PIPESTATUS[0]}

    i=$(( $i + 1 ))
done


echo "UNCOMMITTED LINEAGE" >> $latency_file
for worker in `cat ~/workers.txt`
do
  ssh -o "StrictHostKeyChecking no" -i ~/ray_bootstrap_key.pem $worker "grep 'UNCOMMITTED' /tmp/ray/*/logs/raylet.err" | awk -F':' '{ print $5 }' >> $raylet_log_file
done

sort $raylet_log_file | uniq -c | sort -bgr >> $latency_file
rm $raylet_log_file
