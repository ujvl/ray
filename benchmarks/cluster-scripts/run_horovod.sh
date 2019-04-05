#!/bin/bash

NUM_WORKERS=$1
NUM_GPUS_PER_WORKER=${2:-4}
BATCH_SIZE=${3:-64}
WORKER_IPS=$(tail -n $NUM_WORKERS ~/workers.txt | tac)
WORKER_TO_KILL=$(tail -n 1 ~/workers.txt)


servers=""
for worker in `cat ~/workers.txt`
do
    servers=$servers$worker":$NUM_GPUS_PER_WORKER,"
done


LOG_FILE="$NUM_WORKERS-workers-$BATCH_SIZE-batch-failure-horovod.out"

echo "Logging to log file $LOG_FILE"
echo "Killing worker $WORKER_TO_KILL"
horovodrun -np $(( $NUM_WORKERS * $NUM_GPUS_PER_WORKER )) -H ${servers:0:-1} \
    python ~/tf-benchmarks/scripts/tf_cnn_benchmarks/tf_cnn_benchmarks.py \
        --model resnet101 \
        --batch_size $BATCH_SIZE \
        --variable_update horovod \
        --save_model_steps 640 \
        --train_dir /tmp/horovod-checkpoints \
        --node_to_kill $WORKER_TO_KILL \
        --num_batches 3000 \
        --fail_at 1200 2>&1 | tee $LOG_FILE
