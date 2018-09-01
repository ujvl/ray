sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
conf="`cd "$sbin/../conf/"; pwd`"

# move to another script
#$sbin/sync ~/ray/benchmarks/stream/conf/
#$sbin/sync ~/ray/python/ray/actor.py

# restart head
ulimit -n 65536 
ray stop
$sbin/ray_start_head.sh

# restart workers
$sbin/hosts.sh $sbin/ray_stop_worker.sh
$sbin/hosts.sh $sbin/ray_start_worker.sh
