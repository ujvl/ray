#!/bin/bash

CONFIG=$1

ray get_worker_ips $CONFIG > workers.txt
echo "Found `wc -l workers.txt | awk '{ print $1 }'` workers"
scp workers.txt ubuntu@`ray get_head_ip $CONFIG`:~
echo `ray get_head_ip $CONFIG`
