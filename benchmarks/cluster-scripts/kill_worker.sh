#!/bin/bash

HEAD_IP=$1
WORKER_IP=$2
GCS_DELAY_MS=$3
NODE_RESOURCE=$4


ssh -o "StrictHostKeyChecking=no" $WORKER_IP ray stop

ssh -o "StrictHostKeyChecking=no" $WORKER_IP 'bash -s - '$HEAD_IP $GCS_DELAY_MS $NODE_RESOURCE' 0'< /home/ubuntu/ray/benchmarks/cluster-scripts/start_worker.sh
