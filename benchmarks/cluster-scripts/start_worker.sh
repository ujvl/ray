#!/bin/bash

HEAD_IP=$1
NODE_RESOURCE=${2:-'Node'$RANDOM}
SLEEP_TIME=${3:-$(( $RANDOM % 5 ))}

SEND_THREADS=8
RECEIVE_THREADS=8


export PATH=/home/ubuntu/anaconda3/bin/:$PATH

ulimit -c unlimited
ulimit -n 65536
ulimit -a

echo "Sleeping for $SLEEP_TIME..."
sleep $SLEEP_TIME

ray start --redis-address=$HEAD_IP:6379 \
    --num-cpus 2 \
    --resources='{"'$NODE_RESOURCE'": 100}' \
    --plasma-directory=/mnt/hugepages \
    --huge-pages \
    --internal-config='{
    "initial_reconstruction_timeout_milliseconds": 200,
    "num_heartbeats_timeout": 20,
    "async_message_max_buffer_size": 100,
    "object_manager_repeated_push_delay_ms": 1000,
    "object_manager_pull_timeout_ms": 1000,
    "object_manager_send_threads": '$SEND_THREADS',
    "object_manager_receive_threads": '$RECEIVE_THREADS'}'

sleep 5
taskset -pc 0 `pgrep raylet`
taskset -pc 0 `pgrep plasma_store`
sudo renice -n -19 -p `pgrep raylet`

for pid in `pgrep -w python`; do
    taskset -pc 1-3 $pid
    #sudo renice -n -5 -p $pid
done
