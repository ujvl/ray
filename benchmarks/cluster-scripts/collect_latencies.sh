#!/bin/bash

LATENCY_FILENAME=$1


num_workers=$(( `wc -l ~/workers.txt | awk '{ print $1 }'` - 1 ))
for worker in `tail -n $num_workers ~/workers.txt`; do
    echo $worker
    ssh -o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem $worker "grep LATENCY /tmp/ray/*/logs/worker*" | awk -F'LATENCY sink ' '{ print $2 }' >> $LATENCY_FILENAME
done
wait

