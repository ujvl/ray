#!/bin/bash

parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem"  "ray stop && rm -r /tmp/ray/* && pkill yes"
