for worker in `cat ~/workers.txt`; do
  ssh -o "StrictHostKeyChecking no" -i ~/ray_bootstrap_key.pem $worker $1 &
done
wait
