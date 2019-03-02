#!/bin/bash

HEAD_IP=$1
SLEEP_TIME=${2:-$(( $RANDOM % 5 ))}


export PATH=/home/ubuntu/anaconda3/bin/:$PATH

ulimit -c unlimited
ulimit -n 65536
ulimit -a

echo "Sleeping for $SLEEP_TIME..."
sleep $SLEEP_TIME

ray start --redis-address=$HEAD_IP:6379 --resources='{"Node'$RANDOM'": 100}' --plasma-directory=/mnt/hugepages --huge-pages
