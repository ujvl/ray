#!/bin/bash

HEAD_IP=$1
WORKER_IP=$2
USE_GCS_ONLY=$3
GCS_DELAY_MS=$4
NODE_RESOURCE=$5


ssh -o "StrictHostKeyChecking=no"  -i /home/ubuntu/ray_bootstrap_key.pem $WORKER_IP ray stop

ssh -o "StrictHostKeyChecking=no"  -i /home/ubuntu/ray_bootstrap_key.pem $WORKER_IP 'bash -s - '$HEAD_IP $USE_GCS_ONLY $GCS_DELAY_MS $NODE_RESOURCE' 0'< /home/ubuntu/ray/benchmarks/cluster-scripts/start_worker.sh
