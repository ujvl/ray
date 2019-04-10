#!/bin/bash


NUM_RAYLETS=$1
HEAD_IP=$2
USE_GCS_ONLY=$4
GCS_DELAY_MS=$5
NONDETERMINISM=$6

NUM_SHARDS=1
NUM_TASKS=100
NUM_ITERATIONS=10

if [[ $# -eq 6 ]]
then
    echo "Running with $NUM_RAYLETS workers, use gcs only? $USE_GCS_ONLY, $GCS_DELAY_MS ms GCS delay, nondeterminism? $NONDETERMINISM"
else
    echo "Usage: ./run_job.sh <num raylets> <head IP address> <use gcs only> <GCS delay ms> <nondeterminism> <num shards>"
    exit
fi
latency_prefix="latency-"$NUM_RAYLETS"-workers-"$USE_GCS_ONLY"-gcs-"$GCS_DELAY_MS"-gcsdelay-"$NONDETERMINISM"-nondeterminism-"

if ls $latency_prefix* 1> /dev/null 2>&1
then
    echo "Latency file with prefix $latency_prefix already found, skipping..."
    exit
fi

latency_prefix=$latency_prefix`date +%y-%m-%d-%H-%M-%S`
latency_file=$latency_prefix.txt
raylet_log_file=$latency_prefix.out

bash -x ./cluster-scripts/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM

echo "Logging to file $latency_file..."
cmd="python ring_microbenchmark.py --num-workers $NUM_RAYLETS --num-tasks $NUM_TASKS --num-iterations $NUM_ITERATIONS --redis-address $HEAD_IP:6379 --gcs-delay $GCS_DELAY_MS --latency-file $latency_file"

if [[ $NONDETERMINISM -eq 1 ]]
then
  cmd=$cmd" --nondeterminism"
fi
if [[ $USE_GCS_ONLY -eq 1 ]]
then
  cmd=$cmd" --gcs-only"
fi

echo $cmd | tee $latency_file
$cmd 2>&1 | tee -a $latency_file


echo "UNCOMMITTED LINEAGE" >> $latency_file
for worker in `cat ~/workers.txt`
do
  ssh -o "StrictHostKeyChecking no" -i ~/ray_bootstrap_key.pem $worker "grep 'UNCOMMITTED' /tmp/ray/*/logs/raylet.err" >> $raylet_log_file
done

awk -F':' '{ print $5 }' $raylet_log_file | sort | uniq -c | sort -bgr >> $latency_file
#rm $raylet_log_file
