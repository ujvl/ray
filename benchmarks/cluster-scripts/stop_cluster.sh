#!/bin/bash

parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-oStrictHostKeyChecking=no -i /home/ubuntu/ray_bootstrap_key.pem" "ray stop && rm -r /tmp/ray/* && pkill yes"
parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-oStrictHostKeyChecking=no -i /home/ubuntu/ray_bootstrap_key.pem" "rm -r /tmp/ray-checkpoints"
