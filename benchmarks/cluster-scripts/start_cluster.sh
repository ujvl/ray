#!/bin/bash

NUM_RAYLETS=$1
NUM_REDIS_SHARDS=${2:-1}
#LINEAGE_POLICY=$2
#MAX_LINEAGE_SIZE=$3
#GCS_DELAY_MS=$4

HEAD_IP=$(head -n 1 ~/workers.txt)
WORKER_IPS=$(tail -n $NUM_RAYLETS ~/workers.txt)

if [[ $# -le 2 && $# -ge 1 ]]
then
	echo "Starting cluster with $NUM_RAYLETS raylets, $NUM_REDIS_SHARDS Redis shards..."
else
    echo "Usage: ./start_cluster.sh <num raylets> <num redis shards>"
    exit
fi

./stop_cluster.sh

echo "Starting head with $NUM_REDIS_SHARDS Redis shards..."
./start_head.sh $NUM_REDIS_SHARDS
echo "Done starting head"

echo "Starting workers $WORKER_IPS with GCS delay $GCS_DELAY_MS and $NUM_RAYLETS raylets..."
parallel-ssh -t 0 -i -P -H "$WORKER_IPS" -O "StrictHostKeyChecking=no" -I 'bash -s - '$HEAD_IP < start_worker.sh
echo "Done starting workers"

#sleep 10
#parallel-ssh -t 0 -i -P -H "$WORKER_IPS" -O "StrictHostKeyChecking=no" -O "IdentityFile=~/devenv-key.pem" 'taskset -p -c 0,1 `pgrep raylet`'