sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
conf="`cd "$sbin/../conf/"; pwd`"

HOSTNAME=`hostname`
RAY_HEAD_IP=`cat $conf/head`
RAY_HEAD_PORT=6379

ulimit -n 65536 
#ray=/home/ubuntu/.local/bin/ray
ray="ray"
$ray start --redis-address=$RAY_HEAD_IP:$RAY_HEAD_PORT --object-manager-port=8076 \
          --resources='{"'$HOSTNAME'": 100}' --use-raylet \
	  --huge-pages --plasma-directory=/mnt/hugepages
