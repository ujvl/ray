from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import subprocess
import time

import ray

# Parameters
rounds = 10
latency_filename = "results/latencies"
throughput_filename = "results/throughputs"
_dump_filename = "results/dump"
sample_period = 100
record_type = "int"
record_size = None
max_queue_size = [10,100,1000]        # in number of batches
max_batch_size = [1000,10000]         # in number of records
batch_timeout = [0.05,0.1]
prefetch_depth = 1
background_flush = False
num_stages = [1,2,5,10,15,20]
max_reads_per_second = float("inf")
partitioning = "round_robin"                # "shuffle", "broadcast"
dataflow_parallelism = [1]
cluster_nodes = [1,2,3,4]
source_rate = [10000,20000,40000,-1]
redis_shards = 1
redis_max_memory = 10**10
plasma_memory = 20**10

# Task- and queue-based execution micro-benchmark
cluster_config = "--redis-shards " + str(redis_shards) + " "
cluster_config +=  "--redis-max-memory " + str(redis_max_memory) + " "
cluster_config +=  "--plasma-memory " + str(plasma_memory) + " "
times = "--rounds " + str(rounds) + " "
period = "--sample-period " + str(sample_period) + " "
lf = "--latency-file " + latency_filename + " "
tf = "--throughput-file " + throughput_filename + " "
pd = "--prefetch-depth " + str(prefetch_depth) + " "
cmd_queues = "python batched_queue_benchmark.py " + times + period + lf + tf
cmd_queues += cluster_config
cmd = "python api_benchmark.py " + times + period + lf + tf
cmd += cluster_config
for nodes in cluster_nodes:
    arg0 = "--nodes " + str(nodes) + " "
    for num in num_stages:
        arg1 = "--num-stages " + str(num) + " "
        for queue_size in max_queue_size:
            arg2 = "--queue-size " + str(queue_size) + " "
            for batch_size in max_batch_size:
                arg3 = "--batch-size " + str(batch_size) + " "
                for batch_time in batch_timeout:
                    arg4 = "--flush-timeout " + str(batch_time) + " "
                    for rate in source_rate:
                        arg5 = "--source-rate " + str(rate) + " "
                    # Plain-queue experiment
                    run = cmd_queues + arg0 + arg1 + arg2 + arg3 + arg4 + arg5
                    code = subprocess.call(run, shell=True,
                                           stdout=subprocess.PIPE)
                    for p in dataflow_parallelism:
                        arg6 = "--dataflow-parallelism " + str(p) + " "
                        # Queue-based execution
                        run = cmd + arg0 + arg1 + arg2 + arg3 + arg4 + arg5
                        run += arg6
                        code = subprocess.call(run, shell=True,
                                               stdout=subprocess.PIPE)
                        # Task-based execution
                        run += "--task-based"
                        code = subprocess.call(run, shell=True,
                                               stdout=subprocess.PIPE)
