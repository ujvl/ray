for host in $(cat workers.txt); do
  ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $host < stop_worker.sh
done
