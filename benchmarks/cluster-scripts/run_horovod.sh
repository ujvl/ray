#!/bin/bash

NUM_WORKERS=$1
NUM_GPUS_PER_WORKER=${2:-4}
BATCH_SIZE=${3:-64}
FAILURE=${4:-0}

WORKER_IPS=$(head -n $NUM_WORKERS ~/workers.txt | tac)
WORKER_TO_KILL=$(head -n 1 ~/workers.txt)


servers=""
for worker in $WORKER_IPS
do
    servers=$servers$worker":$NUM_GPUS_PER_WORKER,"
done


parallel-ssh -t 0 -i -P -H "$WORKER_IPS" -x "-oStrictHostKeyChecking=no -i /home/ubuntu/ray_bootstrap_key.pem" "rm /tmp/horovod-checkpoints/*"
parallel-ssh -t 0 -i -P -H "$WORKER_IPS" -x "-oStrictHostKeyChecking=no -i /home/ubuntu/ray_bootstrap_key.pem" "pkill -9 python"
parallel-ssh -t 0 -i -P -H "$WORKER_IPS" -x "-oStrictHostKeyChecking=no -i /home/ubuntu/ray_bootstrap_key.pem" "pkill -9 python"

LOG_FILE="horovod-"$NUM_RAYLETS"-workers-"$BATCH_SIZE"-batch-failure-`date +%y-%m-%d-%H-%M-%S`.out"
NUM_ITERATIONS=100
CHECKPOINT_INTERVAL=100
if [[ $FAILURE -eq 1 ]]; then
    LOG_FILE=failure-$LOG_FILE
    NUM_ITERATIONS=150
    FAIL_AT=15
    CHECKPOINT_INTERVAL=10
fi
echo "Logging to file $LOG_FILE"

echo "Logging to log file $LOG_FILE"
echo "Killing worker $WORKER_TO_KILL"

cmd="horovodrun -np $(( $NUM_WORKERS * $NUM_GPUS_PER_WORKER )) -H ${servers:0:-1} /home/ubuntu/anaconda3/envs/tensorflow_p36/bin/python /home/ubuntu/tf-benchmarks/scripts/tf_cnn_benchmarks/tf_cnn_benchmarks.py --model resnet101 --batch_size $BATCH_SIZE --variable_update horovod --display_every 1 --num_warmup_batches 0 --train_dir /tmp/horovod-checkpoints --save_model_steps $CHECKPOINT_INTERVAL --num_batches $NUM_ITERATIONS"

if [[ $FAILURE -eq 1 ]]
then
    failure_cmd=$cmd" --node_to_kill $WORKER_TO_KILL --fail_at $FAIL_AT"
    echo "Running $failure_cmd" | tee -a $LOG_FILE
    $failure_cmd 2>&1 | tee -a $LOG_FILE
fi

echo "Running $cmd" | tee -a $LOG_FILE
$cmd 2>&1 | tee -a $LOG_FILE
