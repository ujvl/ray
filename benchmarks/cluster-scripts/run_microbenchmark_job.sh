#!/bin/bash

NUM_RAYLETS=$1
HEAD_IP=$2
USE_GCS_ONLY=$3
GCS_DELAY_MS=$4
NONDETERMINISM=$5
NUM_SHARDS=$6
TASK_DURATION=$7
MAX_FAILURES=$8
OUTPUT_DIR=${9:-"$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"}

HOME_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )

NUM_TASKS=100
NUM_ITERATIONS=1

if [[ $# -eq 8 || $# -eq 9 ]]
then
    echo "Running with $NUM_RAYLETS workers, use gcs only? $USE_GCS_ONLY, $GCS_DELAY_MS ms GCS delay, nondeterminism? $NONDETERMINISM"
else
    echo "Usage: ./run_job.sh <num raylets> <head IP address> <use gcs only> <GCS delay ms> <nondeterminism> <num shards> <task duration> <num failures> <output dir>"
    exit
fi
output_prefix=$OUTPUT_DIR"/latency-"$NUM_RAYLETS"-workers-"$NUM_SHARDS"-shards-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-"$NONDETERMINISM"-nondeterminism-"$TASK_DURATION"-task-"$MAX_FAILURES"-failures-"

if ls $output_prefix* 1> /dev/null 2>&1
then
    echo "Latency file with prefix $output_prefix already found, skipping..."
    exit
fi

output_prefix=$output_prefix`date +%y-%m-%d-%H-%M-%S`
output_file=$output_prefix.txt
raylet_log_file=$output_prefix.out

exit_code=1
i=0
while [[ $exit_code -ne 0 ]]
do
    echo "Attempt #$i"
    if [[ $i -eq 3 ]]
    then
        echo "Failed 3 attempts " >> $output_file
        exit
    fi

    bash -x $HOME_DIR/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $MAX_FAILURES

    echo "Logging to file $output_file..."
    cmd="python $HOME_DIR/../ring_microbenchmark.py --num-workers $NUM_RAYLETS --num-tasks $NUM_TASKS --num-iterations $NUM_ITERATIONS --redis-address $HEAD_IP:6379 --gcs-delay $GCS_DELAY_MS --latency-file $output_file --task-duration 0."$TASK_DURATION

    if [[ $NONDETERMINISM -eq 1 ]]
    then
      cmd=$cmd" --nondeterminism"
    fi
    if [[ $USE_GCS_ONLY -eq 1 ]]
    then
      cmd=$cmd" --gcs-only"
    fi

    echo $cmd | tee -a $output_file
    $cmd 2>&1 | tee -a $output_file

    exit_code=${PIPESTATUS[0]}

    i=$(( $i + 1 ))
done


echo "UNCOMMITTED LINEAGE" >> $output_file
for worker in `cat ~/workers.txt`
do
  ssh -o "StrictHostKeyChecking no" -i ~/ray_bootstrap_key.pem $worker "grep 'UNCOMMITTED' /tmp/ray/*/logs/raylet.err" | awk -F':' '{ print $5 }' >> $raylet_log_file
done

sort $raylet_log_file | uniq -c | sort -bgr >> $output_file
rm $raylet_log_file

