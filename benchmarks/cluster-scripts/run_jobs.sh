#!/bin/bash

HEAD_IP=$1

NUM_RAYLETS=64
NUM_SHARDS=1

TASK_DURATION=0

for GCS_DELAY_MS in 0 1 10; do 
    for USE_GCS_ONLY in 0 1; do
        for NONDETERMINISM in 1 0; do
            if [[ $NONDETERMINISM -eq 1 && $USE_GCS_ONLY -eq 0 ]]; then
                for MAX_FAILURES in -1 32 16 8; do
                    bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $HEAD_IP $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $NUM_SHARDS $TASK_DURATION $MAX_FAILURES
                done
            else
                MAX_FAILURES=1
                bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $HEAD_IP $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $NUM_SHARDS $TASK_DURATION $MAX_FAILURES
            fi
        done
    done
done
