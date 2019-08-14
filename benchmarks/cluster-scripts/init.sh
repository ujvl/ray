#cd ~/ray/python && pip install -e . --verbose!/bin/bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "export LC_ALL=C.UTF-8" >> ~/.bashrc
echo "export LANG=C.UTF-8" >> ~/.bashrc
echo "export PATH="$HOME/anaconda3/bin:$PATH"" >> ~/.bashrc

for host in $(cat ~/workers.txt); do
  ssh-keygen -f "/home/ubuntu/.ssh/known_hosts" -R $host
  ssh -o "StrictHostKeyChecking no" -i ~/ray_bootstrap_key.pem $host 'uptime'
  if ! grep "$host$" ~/.ssh/config >1 /dev/null 2>&1; then
      echo "Host $host" >> ~/.ssh/config
      echo "    ForwardAgent yes" >> ~/.ssh/config
  fi
done

parallel-ssh -t 0 -i -P -h ~/workers.txt -x "-o StrictHostKeyChecking=no -i ~/ray_bootstrap_key.pem" -I < $DIR/enable_hugepages.sh
parallel-ssh -t 0 -i -P -h ~/workers.txt -O "StrictHostKeyChecking=no" "sudo apt install -y scala pdsh"

pushd .
git clone git@github.com:stephanie-wang/mpi-bench.git ~/mpi-bench
cd ~/mpi-bench
bash -x ./build.sh
popd

pushd .
cd ~/flink-wordcount
git fetch
git checkout origin/flink-wordcount
popd

num_workers=$(( `wc -l ~/workers.txt | awk '{ print $1 }'` - 1 ))
for worker in `tail -n $num_workers ~/workers.txt`; do
    echo $worker
    rsync -e "ssh -o StrictHostKeyChecking=no" -az "/home/ubuntu/mpi-bench" $worker:/home/ubuntu & sleep 0.5
done
wait

# Run upgrade so that all workers have the same branch checked out.
bash $DIR/upgrade_ray.sh
parallel-ssh -t 0 -i -P -h ~/workers.txt -O "StrictHostKeyChecking=no" -I < $DIR/init_sgd_worker.sh
