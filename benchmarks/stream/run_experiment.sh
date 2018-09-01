rm -rf /tmp/raylogs/
./sbin/ray_restart_cluster.sh
python ysb_stream_bench.py --exp-time 300 --warmup-time 60 --actor-checkpointing --num-parsers 2 --redis-address 172.31.10.214:6379 --num-nodes 32

