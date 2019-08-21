#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

HEAD_IP=$1
NUM_RAYLETS=${2:-64}
OUTPUT_DIR=${3:-"$DIR/latency-$(date +"%y-%m-%d-%H-%M-%S")"}
NUM_SHARDS=1
TASK_DURATION=0

if [[ $# -ne 1 && $# -ne 2 && $# -ne 3 ]]
then
    echo "Usage: ./run_jobs.sh <head IP address> <num raylets> <output dir>"
    exit
fi

# Forward lineage to 1/4 of the nodes, or 1 other node if there are fewer than
# 4 nodes.
PARTIAL_FORWARDING=$(( $NUM_RAYLETS / 4 ))
if [[ $PARTIAL_FORWARDING -lt 1 ]]
then
    PARTIAL_FORWARDING=1
fi

echo "Creating output directory $OUTPUT_DIR..."
mkdir $OUTPUT_DIR

for GCS_DELAY_MS in 0 1 5; do 
    for USE_GCS_ONLY in 0 1; do
        for NONDETERMINISM in 1 0; do
            if [[ $NONDETERMINISM -eq 1 && $USE_GCS_ONLY -eq 0 ]]; then
                for MAX_FAILURES in -1 $PARTIAL_FORWARDING; do
                    bash -x $DIR/run_microbenchmark_job.sh $NUM_RAYLETS $HEAD_IP $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $NUM_SHARDS $TASK_DURATION $MAX_FAILURES $OUTPUT_DIR
                done
            else
                MAX_FAILURES=1
                bash -x $DIR/run_microbenchmark_job.sh $NUM_RAYLETS $HEAD_IP $USE_GCS_ONLY $GCS_DELAY_MS $NONDETERMINISM $NUM_SHARDS $TASK_DURATION $MAX_FAILURES $OUTPUT_DIR
            fi
        done
    done
done