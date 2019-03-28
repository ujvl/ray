from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import subprocess
import time

import ray

# Parameters
rounds = 5
latency_filename = "results/latencies"
throughput_filename = "results/throughputs"
_dump_filename = "results/dump"
sample_period = 100
record_type = "int"
record_size = None
max_queue_size = [100]        # in number of batches
max_batch_size = [1000]       # in number of records
batch_timeout = [0.1]
prefetch_depth = 10
background_flush = False
num_stages = [1,2,5,10,15,20]
max_reads_per_second = float("inf")
partitioning = "round_robin"                # "shuffle", "broadcast"
dataflow_parallelism = [1]
fan_in = [2,4,8,16]
fan_out = [2,4,8,16]
source_rate = [40000]

# Task- and queue-based execution micro-benchmark
times = "--rounds " + str(rounds) + " "
period = "--sample-period " + str(sample_period) + " "
lf = "--latency-file " + latency_filename + " "
tf = "--throughput-file " + throughput_filename + " "
cmd_queues = "python batched_queue_benchmark.py " + times + period + lf + tf
cmd = "python api_benchmark.py " + times + period + lf + tf
for num in num_stages:
    arg1 = "--num-stages " + str(num) + " "
    for queue_size in max_queue_size:
        arg2 = "--queue-size " + str(queue_size) + " "
        for batch_size in max_batch_size:
            arg3 = "--batch-size " + str(batch_size) + " "
            for batch_time in batch_timeout:
                arg4 = "--flush-timeout " + str(batch_time) + " "
                for rate in source_rate:
                    arg5 = "--max-source-rate " + str(rate) + " "
                # Plain-queue experiment
                run = cmd_queues + arg1 + arg2 + arg3 + arg4 + arg5
                code = subprocess.call(run, shell=True,
                                    stdout=subprocess.PIPE)
                for lop in dataflow_parallelism:
                    arg6 = "--dataflow-parallelism " + str(lop) + " "
                    # Queue-based execution
                    run = cmd + arg1 + arg2 + arg3 + arg4 + arg5 + arg6
                    code = subprocess.call(run, shell=True,
                                        stdout=subprocess.PIPE)
                    # Task-based execution
                    run += "--task-based"
                    code = subprocess.call(run, shell=True,
                                        stdout=subprocess.PIPE)
