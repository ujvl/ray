for worker in `tail -n +2 ~/workers.txt`; do
  scp -o "StrictHostKeyChecking no" -i ~/ray_bootstrap_key.pem $1 $worker:$1
done
