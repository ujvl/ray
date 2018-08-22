for host in $(cat workers.txt); do
  ssh -o "StrictHostKeyChecking no" -i ~/devenv-key.pem $host uptime
done
