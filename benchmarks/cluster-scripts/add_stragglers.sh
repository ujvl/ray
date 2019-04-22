#!/bin/bash

NUM_STRAGGLERS=$1

STRAGGLER_IPS=$(tail -n $NUM_STRAGGLERS ~/workers.txt)

parallel-ssh -t 0 -i -P -H "$STRAGGLER_IPS" -x "-o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem" 'for i in `seq 1 4`; do yes > /dev/null & done'
