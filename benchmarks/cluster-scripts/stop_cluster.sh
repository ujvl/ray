#!/bin/bash

parallel-ssh -t 0 -i -P -h ~/workers.txt -O "StrictHostKeyChecking=no" "ray stop && rm -r /tmp/ray/* && pkill yes"
parallel-ssh -t 0 -i -P -h ~/workers.txt -O "StrictHostKeyChecking=no" "rm -r /tmp/ray-checkpoints"
