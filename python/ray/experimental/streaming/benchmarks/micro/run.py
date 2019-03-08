import subprocess
import time

import ray

# Parameters
rounds = 4
latency_filename = "data/latencies.txt"
throughput_filename = "data/throughputs.txt"
sample_period = 100
record_type = "int"
record_size = None
max_queue_size = [100,1000,10000,100000]    # in number of batches
max_batch_size = [100,1000,10000]           # in number of records
batch_timeout = [0.01, 0.1]
prefetch_depth = 10
background_flush = False
num_queues = [1,2,3,4,5,6,7,8,9,10]
num_stages = [1,2,3,4,5,6,7,8,9,10]
max_reads_per_second = float("inf")
task_based = [0,1]
partitioning = "round_robin"                # "shuffle", "broadcast"
dataflow_parallelism = [1,2]
fan_in = [2,4,8,16]
fan_out = [2,4,8,16]

# Batched queue micro-benchmark
times = "--rounds " + str(rounds) + " "
period = "--sample-period " + str(100) + " "
lf = "--latency-file " + latency_filename + " "
tf = "--throughput-file " + throughput_filename + " "
command = "python batched_queue_benchmark.py " + times + period + lf + tf
for num in num_queues:
    arg1 = "--num-queues " + str(num) + " "
    for queue_size in max_queue_size:
        arg2 = "--queue-size " + str(queue_size) + " "
        for batch_size in max_batch_size:
            arg3 = "--batch-size " + str(batch_size) + " "
            for batch_time in batch_timeout:
                arg4 = "--flush-timeout " + str(batch_time) + " "
                run = command + arg1 + arg2 + arg3 + arg4
                print("Executing: ", run)
                code = subprocess.call(run, shell=True,
                                    stdout=subprocess.PIPE)

# Chaining micro-benchmark
times = "--rounds " + str(rounds) + " "
period = "--sample-period " + str(100) + " "
lf = "--latency-file " + latency_filename + " "
tf = "--throughput-file " + throughput_filename + " "
command = "python api_benchmark.py " + times + period + lf + tf
for num in num_queues:
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
                    print("Executing: ", run)
                    code = subprocess.call(run, shell=True,
                                        stdout=subprocess.PIPE)
                    # task-based
                    run += "--task-based 1"
                    print("Executing: ", run)
                    code = subprocess.call(run, shell=True,
                                        stdout=subprocess.PIPE)


# Fan-in/out micro-benchmark
times = "--rounds " + str(rounds) + " "
period = "--sample-period " + str(100) + " "
lf = "--latency-file " + latency_filename + " "
tf = "--throughput-file " + throughput_filename + " "
command = "python fan_in_out_benchmark.py " + times + period + lf + tf
for queue_size in max_queue_size:
    arg2 = "--queue-size " + str(queue_size) + " "
    for batch_size in max_batch_size:
        arg3 = "--batch-size " + str(batch_size) + " "
        for batch_time in batch_timeout:
            arg4 = "--flush-timeout " + str(batch_time) + " "
            for fin in fan_in:
                arg5 = "--fan-in " + str(fin) + " "
                # queue-based
                run = command + arg2 + arg3 + arg4 + arg5
                print("Executing: ", run)
                code = subprocess.call(run, shell=True,
                                    stdout=subprocess.PIPE)
                # task-based
                run += "--task-based 1"
                print("Executing: ", run)
                os.system(run)
            for fout in fan_out:
                arg5 = "--fan-out " + str(fout) + " "
                # queue-based
                run = command + arg2 + arg3 + arg4 + arg5
                print("Executing: ", run)
                code = subprocess.call(run, shell=True,
                                    stdout=subprocess.PIPE)
                # task-based
                run += "--task-based 1"
                print("Executing: ", run)
                os.system(run)
