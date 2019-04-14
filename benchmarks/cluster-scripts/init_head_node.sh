#!/bin/bash

CONFIG=$1

ray get-worker-ips $CONFIG > workers.txt
echo "Found `wc -l workers.txt | awk '{ print $1 }'` workers"
scp -i ~/.ssh/ray-autoscaler_us-west-2.pem workers.txt ubuntu@`ray get-head-ip $CONFIG`:~
echo `ray get-head-ip $CONFIG`
