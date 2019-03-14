#!/bin/bash

HEAD_IP=$1
NUM_ITERATIONS=100

for NUM_RAYLETS in 48 24 12; do
    for SIZE in 2500000 25000000 250000000; do
        for GCS_DELAY_MS in -1 0 1; do
            for NUM_SHARDS in 2 4 8; do
                bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $SIZE $GCS_DELAY_MS $NUM_SHARDS $HEAD_IP
            done
        done
    done
done

for NUM_RAYLETS in 48 24 12; do
    for SIZE in 2500000 25000000 250000000; do
        for GCS_DELAY_MS in -1 0 1; do
            for NUM_SHARDS in 2 4 8; do
                bash -x ./cluster-scripts/run_job.sh $NUM_RAYLETS $SIZE $GCS_DELAY_MS $NUM_SHARDS $HEAD_IP 1
            done
        done
    done
done
