#!/bin/bash

NUM_REDIS_SHARDS=$1
GCS_DELAY_MS=$2

export PATH=/home/ubuntu/anaconda3/bin/:$PATH
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

ulimit -c unlimited
ulimit -n 65536
ulimit -a
ray start --head \
  --redis-port=6379 \
  --num-redis-shards $NUM_REDIS_SHARDS \
  --resources='{"Node_0": 100}' \
  --plasma-directory=/mnt/hugepages \
  --object-store-memory 8000000000 \
  --huge-pages \
  --internal-config='{
    "initial_reconstruction_timeout_milliseconds": 200,
    "gcs_delay_ms": '$GCS_DELAY_MS',
    "lineage_stash_max_failures": -1,
    "num_heartbeats_timeout": 20,
    "object_manager_repeated_push_delay_ms": 1000,
    "object_manager_pull_timeout_ms": 1000}'
