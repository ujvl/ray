for worker in `tail -n +2 ~/workers.txt`; do
  #ssh -o "StrictHostKeyChecking no" -i ~/ray_bootstrap_key.pem $worker $1 &
  ssh -o "StrictHostKeyChecking no" -i ~/ray_bootstrap_key.pem $worker $"${@// /\\ }" 2>&1 | sed "s/^/$worker: /" &
done
wait
