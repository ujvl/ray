#!/bin/bash

NUM_REDIS_SHARDS=$1
USE_GCS_ONLY=$2
GCS_DELAY_MS=$3
NONDETERMINISM=$4

export PATH=/home/ubuntu/anaconda3/bin/:$PATH

MAX_FAILURES=1
if [[ $NONDETERMINISM -eq 1 ]]
then
  MAX_FAILURES=-1
fi

ulimit -c unlimited
ulimit -n 65536
ulimit -a
ray start --head \
  --redis-port=6379 \
  --num-redis-shards \
  $NUM_REDIS_SHARDS \
  --plasma-directory=/mnt/hugepages \
  --huge-pages \
  --internal-config='{
  "initial_reconstruction_timeout_milliseconds": 200,
  "gcs_delay_ms": '$GCS_DELAY_MS',
  "use_gcs_only": '$USE_GCS_ONLY',
  "lineage_stash_max_failures": '$MAX_FAILURES',
  "log_nondeterminism": '$NONDETERMINISM',
  "num_heartbeats_timeout": 20,
  "object_manager_repeated_push_delay_ms": 1000,
  "object_manager_pull_timeout_ms": 1000}'
