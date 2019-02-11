./sbin/ray_restart_cluster.sh

NUM_NODES=`cat ./conf/priv-hosts-all | wc -l`
python bfs.py --redis-address `hostname`:6379 \
                           --num-nodes $NUM_NODES \
                           --num-subgraphs $NUM_NODES
