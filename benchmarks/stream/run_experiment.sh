rm -rf /tmp/raylogs/

./sbin/ray_restart_cluster.sh

NUM_NODES=`cat ./conf/priv-hosts-all | wc -l`
#NUM_NODES=1
python ysb_stream_bench.py --exp-time 300 --warmup-time 60 --actor-checkpointing --num-parsers 2 --redis-address `hostname`:6379 --num-nodes $NUM_NODES

