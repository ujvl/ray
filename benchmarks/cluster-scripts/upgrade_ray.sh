#!/bin/bash


CHECKOUT=${1:-0}

if [[ $CHECKOUT -ne 0 ]]
then
    cd ~/ray
    git fetch
    git checkout origin/lineage-stash-allreduce
fi

(cd ~/ray/build && make -j8) || (find /home/ubuntu/ray/src/ray -name *_generated.h -exec rm {} \; && cd ~/ray/build  && make -j8) || (cd ~/ray/python && python setup.py develop)


num_workers=$(( `wc -l ~/workers.txt | awk '{ print $1 }'` - 1 ))
for worker in `tail -n $num_workers ~/workers.txt`; do
    echo $worker
    rsync -e "ssh -o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem" -az "/home/ubuntu/ray" $worker:/home/ubuntu & sleep 0.5
done
wait

parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem" "pip install -e ~/ray/examples/cython/"
