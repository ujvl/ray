#!/bin/bash
set -x -e
NUM_REDIS_SHARDS=$1
USE_GCS_ONLY=$2
GCS_DELAY_MS=$3
NONDETERMINISM=$4
MAX_FAILURES=${5:-1}

#source activate tensorflow_p36
#export PATH=/home/ubuntu/anaconda3/envs/tensorflow_p36/bin/:$PATH
export PATH=/home/ubuntu/anaconda3/bin/:$PATH
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

SLEEP_TIME=${5:-$(( $RANDOM % 5 ))}

SEND_THREADS=1
RECEIVE_THREADS=1

#export RAYLET_PERFTOOLS_PATH=1
#export PERFTOOLS_PATH="/usr/lib/libprofiler.so"
#export PERFTOOLS_LOGFILE="/tmp/pprof.out"

if [[ $NONDETERMINISM -eq 1 && $MAX_FAILURES -eq 1 ]]
then
  MAX_FAILURES=-1
fi

ulimit -c unlimited
ulimit -n 65536
ulimit -a

ray start --head \
  --redis-port=6379 \
  --redis-max-memory 10000000000 \
  --num-cpus 4 \
  --num-redis-shards $NUM_REDIS_SHARDS \
  --internal-config='{
    "initial_reconstruction_timeout_milliseconds": 200,
    "gcs_delay_ms": '$GCS_DELAY_MS',
    "use_gcs_only": '$USE_GCS_ONLY',
    "lineage_stash_max_failures": '$MAX_FAILURES',
    "log_nondeterminism": '$NONDETERMINISM',
    "num_heartbeats_timeout": 20,
    "object_manager_repeated_push_delay_ms": 1000,
    "object_manager_pull_timeout_ms": 1000,
    "object_manager_send_threads": '$SEND_THREADS',
    "object_manager_receive_threads": '$RECEIVE_THREADS'}'

sleep 5

#i=0
#for pid in `pgrep -w raylet`; do
#    core=$(( i % 3  + 1 ))
#    taskset -pc $core $pid
#    sudo renice -n -10 -p $pid
#    i=$(( $i + 1 ))
#done

#taskset -pc 0 `pgrep -x raylet`
#sudo renice -n -19 -p `pgrep -x raylet`
#if [[ $# -ne 3 ]]; then
#    yes > /dev/null &
#    taskset -pc 0 $!
#fi

#for i in `seq 0 7`; do
#    yes > /dev/null &
#    taskset -pc $i $!
#done


#for pid in `pgrep -w python`; do
#    taskset -pc 1-3 $pid
#    #sudo renice -n -5 -p $pid
#done

sleep 1
