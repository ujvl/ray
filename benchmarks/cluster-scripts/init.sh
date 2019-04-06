#cd ~/ray/python && pip install -e . --verbose!/bin/bash

for host in $(cat ~/workers.txt); do
  ssh-keygen -f "/home/ubuntu/.ssh/known_hosts" -R $host
  ssh -o "StrictHostKeyChecking no" $host 'uptime'
  if ! grep "$host$" ~/.ssh/config >1 /dev/null 2>&1; then
      echo "Host $host" >> ~/.ssh/config
      echo "    ForwardAgent yes" >> ~/.ssh/config
  fi
done

parallel-ssh -t 0 -i -P -h ~/workers.txt -O "StrictHostKeyChecking=no" -I < enable_hugepages.sh

# Run upgrade so that all workers have the same branch checked out.
bash ./upgrade_ray.sh
parallel-ssh -t 0 -i -P -h ~/workers.txt -O "StrictHostKeyChecking=no" -I < init_sgd_worker.sh
