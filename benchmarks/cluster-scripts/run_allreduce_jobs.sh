#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

HEAD_IP=$1
NUM_RAYLETS=${2:-64}
OUTPUT_DIR=${3:-"$DIR/allreduce-$(date +"%y-%m-%d-%H-%M-%S")"}

if [[ $# -lt 1 || $# -gt 3 ]]
then
    echo "Usage: ./run_allreduce_jobs.sh <head IP address> <num raylets>"
    exit
fi

echo "Creating output directory $OUTPUT_DIR..."
mkdir $OUTPUT_DIR

# Run the failure experiment.
NUM_FLOAT32=25000000
GCS_DELAY_MS=0
MAX_FAILURES=1
CHECKPOINT_INTERVAL=150
bash -x $DIR/run_allreduce_job.sh $HEAD_IP $NUM_RAYLETS $OUTPUT_DIR $NUM_FLOAT32 0 $MAX_FAILURES $GCS_DELAY_MS $CHECKPOINT_INTERVAL
bash -x $DIR/run_allreduce_job.sh $HEAD_IP $NUM_RAYLETS $OUTPUT_DIR $NUM_FLOAT32 1 $MAX_FAILURES $GCS_DELAY_MS $CHECKPOINT_INTERVAL

# Run latency experiments without failure.
for NUM_NODES in 4 16 32 64; do
    if [[ $NUM_NODES -gt $NUM_RAYLETS ]]
    then
        continue
    fi
    for NUM_FLOAT32 in 25000000; do
        for GCS_DELAY_MS in 0 1 5; do
            USE_GCS=0
            MAX_FAILURES=0
            bash -x $DIR/run_allreduce_job.sh $HEAD_IP $NUM_NODES $OUTPUT_DIR $NUM_FLOAT32 $USE_GCS $MAX_FAILURES $GCS_DELAY_MS

            USE_GCS=0
            MAX_FAILURES=1
            bash -x $DIR/run_allreduce_job.sh $HEAD_IP $NUM_NODES $OUTPUT_DIR $NUM_FLOAT32 $USE_GCS $MAX_FAILURES $GCS_DELAY_MS

            USE_GCS=1
            MAX_FAILURES=0
            bash -x $DIR/run_allreduce_job.sh $HEAD_IP $NUM_NODES $OUTPUT_DIR $NUM_FLOAT32 $USE_GCS $MAX_FAILURES $GCS_DELAY_MS
        done
    done
done
