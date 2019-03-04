#!/bin/bash

HEAD_IP=$1
NODE_RESOURCE=${2:-'Node'$RANDOM}
SLEEP_TIME=${3:-$(( $RANDOM % 5 ))}

SEND_THREADS=2
RECEIVE_THREADS=2


export PATH=/home/ubuntu/anaconda3/bin/:$PATH

ulimit -c unlimited
ulimit -n 65536
ulimit -a

echo "Sleeping for $SLEEP_TIME..."
sleep $SLEEP_TIME

ray start --redis-address=$HEAD_IP:6379 \
    --resources='{"'$NODE_RESOURCE'": 100}' \
    --plasma-directory=/mnt/hugepages \
    --huge-pages \
    --internal-config='{
    "initial_reconstruction_timeout_milliseconds": 200,
    "num_heartbeats_timeout": 20,
    "object_manager_repeated_push_delay_ms": 1000,
    "object_manager_send_threads": '$SEND_THREADS',
    "object_manager_receive_threads": '$RECEIVE_THREADS'}'
