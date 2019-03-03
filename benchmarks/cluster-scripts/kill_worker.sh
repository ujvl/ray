#!/bin/bash

HEAD_IP=$1
WORKER_IP=$2
NODE_RESOURCE=$3


ssh -o "StrictHostKeyChecking=no" $WORKER_IP ray stop

ssh -o "StrictHostKeyChecking=no" $WORKER_IP 'bash -s - '$HEAD_IP $NODE_RESOURCE' 0'< /home/ubuntu/ray/benchmarks/cluster-scripts/start_worker.sh
