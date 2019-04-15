#!/bin/bash

parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem" "LC_ALL=C.UTF-8 LANG=C.UTF-8 ray stop && rm -r /tmp/ray/* && pkill yes"
