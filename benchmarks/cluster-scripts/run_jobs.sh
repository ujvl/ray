#!/bin/bash

HEAD_IP=$1

NUM_RAYLETS=64

for NUM_RAYLETS in 64; do
  #for GCS_DELAY_MS in 0 1 2 4 8 16; do
  for GCS_DELAY_MS in 0; do
    for USE_GCS_ONLY in 0 1; do
      for NONDETERMINISM in 0 1; do
        bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $HEAD_IP $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM
        bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $HEAD_IP $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM
      done
    done
  done
done
