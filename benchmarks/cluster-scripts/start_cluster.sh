#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

NUM_RAYLETS=$1
NUM_REDIS_SHARDS=${2:-1}
USE_GCS_ONLY=${3:-0}
GCS_DELAY_MS=${4:-0}
NONDETERMINISM=${5:-0}
MAX_FAILURES=${6:-1}
# If this is more than 0, then hugepages should be used. Otherwise, hugepages
# will be disabled and the default plasma capacity will be used.
OBJECT_STORE_MEMORY_GB=${7:-0}
# When an object does not fit in object store, evict max(object size, total capacity / OBJECT_STORE_EVICTION).
OBJECT_STORE_EVICTION=${8:-20}
PEG=${9:-0}
OBJECT_MANAGER_THREADS=${10:-1}

HEAD_IP=$(head -n 1 ~/workers.txt)
WORKER_IPS=$(tail -n $NUM_RAYLETS ~/workers.txt)

if [[ $# -le 10 && $# -ge 1 ]]
then
	echo "Starting cluster with $NUM_RAYLETS raylets, $NUM_REDIS_SHARDS Redis shards..."
	echo "Testing with GCS: $USE_GCS_ONLY, GCS delay: $GCS_DELAY_MS, nondeterminism: $NONDETERMINISM"
else
    echo "Usage: ./start_cluster.sh <num raylets> <num redis shards> <use gcs only> <GCS delay ms> <nondeterminism>"
    exit
fi

if [[ $NONDETERMINISM -eq 0 && $MAX_FAILURES -ne 1 ]]; then
  echo "Specified deterministic recovery, but max failures was set to $MAX_FAILURES"
  exit
fi

if [[ $USE_GCS_ONLY -eq 1 && $MAX_FAILURES -ne 1 ]]; then
  echo "Specified GCS only, but max failures was set to $MAX_FAILURES"
  exit
fi

bash $DIR/stop_cluster.sh

# 2MB per hugepage.
NUM_HUGEPAGES=$(( $OBJECT_STORE_MEMORY_GB * 1000 / 2))
# Allocate 30% more hugepages than requested in case of fragmentation.
NR_HUGEPAGES=$(awk "BEGIN {print int($NUM_HUGEPAGES * 1.3)}")
echo "$NR_HUGEPAGES"
if [[ `cat /proc/sys/vm/nr_hugepages` -ne $NR_HUGEPAGES ]]
then
    echo "nr_hugepages does not match the requested $NUM_HUGEPAGES, enabling $NR_HUGEPAGES hugepages on all nodes..."
    parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-oStrictHostKeyChecking=no -i /home/ubuntu/ray_bootstrap_key.pem" -I 'bash -s - '$NR_HUGEPAGES < $DIR/enable_hugepages.sh
    if [[ `cat /proc/sys/vm/nr_hugepages` -ne $NR_HUGEPAGES ]]
    then
        echo "Failed to set hugepages to $NR_HUGEPAGES, exiting..."
        exit 1
    fi
fi


echo "Starting head with $NUM_REDIS_SHARDS Redis shards..."
bash $DIR/start_head.sh $NUM_REDIS_SHARDS $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $MAX_FAILURES $OBJECT_STORE_MEMORY_GB
echo "Done starting head"

echo "Starting workers $WORKER_IPS with GCS delay $GCS_DELAY_MS and $NUM_RAYLETS raylets..."
parallel-ssh -t 0 -i -P -H "$WORKER_IPS" -x "-oStrictHostKeyChecking=no -i /home/ubuntu/ray_bootstrap_key.pem" -I 'bash -s - '$HEAD_IP $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $MAX_FAILURES $OBJECT_STORE_MEMORY_GB $OBJECT_STORE_EVICTION $PEG $OBJECT_MANAGER_THREADS < $DIR/start_worker.sh
echo "Done starting workers"

#sleep 10
#parallel-ssh -t 0 -i -P -H "$WORKER_IPS" -O "StrictHostKeyChecking=no" -O "IdentityFile=~/devenv-key.pem" 'taskset -p -c 0,1 `pgrep raylet`'
