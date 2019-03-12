from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import subprocess
import time

import ray

# Parameters
rounds = 5
latency_filename = "results/api/latencies.txt"
throughput_filename = "results/api/throughputs.txt"
dump_filename = "results/api/dump.json"
sample_period = 100
record_type = "int"
record_size = None
max_queue_size = [100,1000,10000,100000]    # in number of batches
max_batch_size = [10,100,1000,10000]           # in number of records
batch_timeout = [0.01, 0.1, 0.001]
prefetch_depth = 10
background_flush = False
num_queues = [2,3,4,5]
num_stages = [2,3,4,5]
max_reads_per_second = float("inf")
partitioning = "round_robin"                # "shuffle", "broadcast"
dataflow_parallelism = [1]
fan_in = [2,4,8,16]
fan_out = [2,4,8,16]

# Batched queue micro-benchmark
times = "--rounds " + str(rounds) + " "
period = "--sample-period " + str(sample_period) + " "
lf = "--latency-file " + latency_filename + " "
tf = "--throughput-file " + throughput_filename + " "
df = "--dump-file " + dump_filename + " "
command = "python batched_queue_benchmark.py " + times + period + lf + tf + df
for num in num_queues:
    arg1 = "--num-queues " + str(num) + " "
    for queue_size in max_queue_size:
        arg2 = "--queue-size " + str(queue_size) + " "
        for batch_size in max_batch_size:
            arg3 = "--batch-size " + str(batch_size) + " "
            for batch_time in batch_timeout:
                arg4 = "--flush-timeout " + str(batch_time) + " "
                run = command + arg1 + arg2 + arg3 + arg4
                code = subprocess.call(run, shell=True,
                                    stdout=subprocess.PIPE)

# Chaining micro-benchmark
times = "--rounds " + str(rounds) + " "
period = "--sample-period " + str(100) + " "
lf = "--latency-file " + latency_filename + " "
tf = "--throughput-file " + throughput_filename + " "
df = "--dump-file " + dump_filename + " "
command = "python api_benchmark.py " + times + period + lf + tf + df
for num in num_stages:
    arg1 = "--num-stages " + str(num) + " "
    for queue_size in max_queue_size:
        arg2 = "--queue-size " + str(queue_size) + " "
        for batch_size in max_batch_size:
            arg3 = "--batch-size " + str(batch_size) + " "
            for batch_time in batch_timeout:
                arg4 = "--flush-timeout " + str(batch_time) + " "
                for parallelism in dataflow_parallelism:
                    arg5 = "--dataflow-parallelism " + str(parallelism) + " "
                    # queue-based
                    run = command + arg1 + arg2 + arg3 + arg4 + arg5
                    code = subprocess.call(run, shell=True,
                                        stdout=subprocess.PIPE)
                    # task-based
                    run += "--task-based"
                    code = subprocess.call(run, shell=True,
                                        stdout=subprocess.PIPE)


# # Fan-in/out micro-benchmark
# times = "--rounds " + str(rounds) + " "
# period = "--sample-period " + str(100) + " "
# lf = "--latency-file " + latency_filename + " "
# tf = "--throughput-file " + throughput_filename + " "
# command = "python fan_in_out_benchmark.py " + times + period + lf + tf
# for queue_size in max_queue_size:
#     arg2 = "--queue-size " + str(queue_size) + " "
#     for batch_size in max_batch_size:
#         arg3 = "--batch-size " + str(batch_size) + " "
#         for batch_time in batch_timeout:
#             arg4 = "--flush-timeout " + str(batch_time) + " "
#             for fin in fan_in:
#                 arg5 = "--fan-in " + str(fin) + " "
#                 # queue-based
#                 run = command + arg2 + arg3 + arg4 + arg5
#                 print("Executing: ", run)
#                 code = subprocess.call(run, shell=True,
#                                     stdout=subprocess.PIPE)
#                 # task-based
#                 run += "--task-based 1"
#                 print("Executing: ", run)
#                 code = subprocess.call(run, shell=True,
#                                     stdout=subprocess.PIPE)
#             for fout in fan_out:
#                 arg5 = "--fan-out " + str(fout) + " "
#                 # queue-based
#                 run = command + arg2 + arg3 + arg4 + arg5
#                 print("Executing: ", run)
#                 code = subprocess.call(run, shell=True,
#                                     stdout=subprocess.PIPE)
#                 # task-based
#                 run += "--task-based 1"
#                 print("Executing: ", run)
#                 code = subprocess.call(run, shell=True,
#                                     stdout=subprocess.PIPE)
