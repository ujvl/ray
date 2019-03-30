#!/bin/bash

NUM_WORKERS=$1
WORKER_IPS=$(tail -n $NUM_WORKERS ~/workers.txt | tac)


hosts=""
for worker in $WORKER_IPS;
do
    hosts=$hosts"$worker:1,"
done
hosts=${hosts:0:-1}
echo $hosts

HOROVOD_TIMELINE=/home/ubuntu/benchmarks/horovod.json horovodrun -np $NUM_WORKERS -H $hosts python ~/benchmarks/scripts/tf_cnn_benchmarks/tf_cnn_benchmarks.py --model resnet101 --batch_size 1 --variable_update horovod
