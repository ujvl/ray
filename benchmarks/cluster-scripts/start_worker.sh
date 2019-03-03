#!/bin/bash

HEAD_IP=$1
NODE_RESOURCE=${2:-'Node'$RANDOM}
SLEEP_TIME=${3:-$(( $RANDOM % 5 ))}


export PATH=/home/ubuntu/anaconda3/bin/:$PATH

ulimit -c unlimited
ulimit -n 65536
ulimit -a

echo "Sleeping for $SLEEP_TIME..."
sleep $SLEEP_TIME

ray start --redis-address=$HEAD_IP:6379 --resources='{"'$NODE_RESOURCE'": 100}' --plasma-directory=/mnt/hugepages --huge-pages --internal-config='{"initial_reconstruction_timeout_milliseconds": 200, "num_heartbeats_timeout": 20, "object_manager_repeated_push_delay_ms": 1000}'
