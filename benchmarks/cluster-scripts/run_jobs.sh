#!/bin/bash

HEAD_IP=$1

NUM_RAYLETS=64
SIZE=25000000
GCS_DELAY_MS=0
bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $HEAD_IP $SIZE 0 $GCS_DELAY_MS 1 1
bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $HEAD_IP $SIZE 1 $GCS_DELAY_MS 1 1

for NUM_RAYLETS in 16 32 64; do
    for SIZE in 2500000 25000000 250000000; do
        for GCS_DELAY_MS in 0 1 5; do
            bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $HEAD_IP $SIZE 0 $GCS_DELAY_MS
            bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $HEAD_IP $SIZE 1 $GCS_DELAY_MS
        done
    done
done
