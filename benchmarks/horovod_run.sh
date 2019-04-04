#!/bin/bash

NUM_WORKERS=$1
NUM_GPUS_PER_WORKER=${2:-4}
BATCH_SIZE=${3:-64}

servers=""
for worker in `cat ~/workers.txt`
do
    servers=$servers$worker":$NUM_GPUS_PER_WORKER,"
done


cd ~/tf-benchmarks
horovodrun -np $(( $NUM_GPUS_PER_WORKER * $NUM_WORKERS )) -H ${servers:0:-1} \
    python scripts/tf_cnn_benchmarks/tf_cnn_benchmarks.py \
        --model resnet101 \
        --batch_size $BATCH_SIZE \
        --variable_update horovod
