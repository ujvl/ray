#!/bin/bash

NUM_REDIS_SHARDS=$1

export PATH=/home/ubuntu/anaconda3/bin/:$PATH

ulimit -c unlimited
ulimit -n 65536
ulimit -a
ray start --head --redis-port=6379 --num-redis-shards $NUM_REDIS_SHARDS --plasma-directory=/mnt/hugepages --huge-pages --internal-config='{"initial_reconstruction_timeout_milliseconds": 200, "num_heartbeats_timeout": 20, "object_manager_repeated_push_delay_ms": 1000}'
