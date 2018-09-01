HOSTNAME=`hostname`
PORT=6379

ulimit -n 65536  
ray start --head --redis-port=$PORT --object-manager-port=8076 \
          --resources='{"'$HOSTNAME'": 100}' --use-raylet \
          --huge-pages --plasma-directory=/mnt/hugepages
