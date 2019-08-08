#!/bin/bash

LATENCY_FILENAME=$1
THROUGHPUT_FILENAME=$2

# Write CSV headers.
echo "sink_id,timestamp,cur_time,latency" >> $LATENCY_FILENAME
echo "sink_id,timestamp,cur_time,throughput" >> $THROUGHPUT_FILENAME

num_workers=$(( `wc -l ~/workers.txt | awk '{ print $1 }'` - 1 ))
for worker in `tail -n $num_workers ~/workers.txt`; do
    echo $worker
    ssh -o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem $worker "grep LATENCY /tmp/ray/*/logs/worker*" | awk -F'LATENCY sink ' '{ print $2 }' >> $LATENCY_FILENAME &
    ssh -o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem $worker "grep THROUGHPUT /tmp/ray/*/logs/worker*" | awk -F'THROUGHPUT sink ' '{ print $2 }' >> $THROUGHPUT_FILENAME &
    wait
done
