#!/bin/bash

NUM_ITERATIONS=100

for NUM_RAYLETS in 12 24 48; do
    for SIZE in 2500000 25000000 250000000; do
        for GCS_DELAY_MS in -1 0; do
            for NUM_SHARDS in 2 4 8; do
                latency_file=latency-$NUM_RAYLETS-workers-$NUM_SHARDS-shards-$GCS_DELAY_MS-gcs-$SIZE-bytes-`date +%h-%d-%M-%S`.txt
                echo "Logging to file $latency_file..."
                bash -x ./cluster-scripts/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS $GCS_DELAY_MS
                python allreduce.py --num-workers $NUM_RAYLETS --size $SIZE --num-iterations $NUM_ITERATIONS --redis-address 172.30.0.29:6379 --latency-file $latency_file
            done
        done
    done
done

for NUM_RAYLETS in 12 24 48; do
    for SIZE in 2500000 25000000 250000000; do
        for GCS_DELAY_MS in -1 0; do
            for NUM_SHARDS in 2 4 8; do
                latency_file=latency-$NUM_RAYLETS-workers-$NUM_SHARDS-shards-$GCS_DELAY_MS-gcs-$SIZE-bytes-failure-`date +%h-%d-%M-%S`.txt
                echo "Logging to file $latency_file..."
                bash -x ./cluster-scripts/start_cluster.sh $NUM_RAYLETS $NUM_SHARDS $GCS_DELAY_MS
                python allreduce.py --num-workers $NUM_RAYLETS --size $SIZE --num-iterations $NUM_ITERATIONS --redis-address 172.30.0.29:6379 --latency-file $latency_file --test-failure
            done
        done
    done
done
