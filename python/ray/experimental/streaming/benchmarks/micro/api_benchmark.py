from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
try:
    from itertools import zip_longest as zip_longest
except:
    from itertools import izip_longest as zip_longest
import logging
import math
import numpy as np
import random
import string
import sys
import time

import ray
from ray.experimental.streaming.batched_queue import BatchedQueue
import ray.experimental.streaming.benchmarks.utils as utils
from ray.experimental.streaming.communication import QueueConfig
from ray.experimental.streaming.streaming import Environment


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

parser = argparse.ArgumentParser()
parser.add_argument("--rounds", default=10,
                    help="the number of experiment rounds")
parser.add_argument("--pin-processes", default=False,
                    action='store_true',
                    help="whether to pin python processes to cores or not")
parser.add_argument("--nodes", default=1,
                    help="total number of nodes in the cluster")
parser.add_argument("--redis-shards", default=1,
                    help="total number of Redis shards")
parser.add_argument("--redis-max-memory", default=10**9,
                    help="max amount of memory per Redis shard")
parser.add_argument("--plasma-memory", default=10**9,
                    help="amount of memory to start plasma with")
# Dataflow-related parameters
parser.add_argument("--num-stages", default=2,
                    help="the number of stages in the chain")
parser.add_argument("--task-based", default=False,
                    action='store_true',
                    help="task-based execution")
parser.add_argument("--dataflow-parallelism", default=1,
                    help="the number of instances per operator")
parser.add_argument("--record-type", default="int",
                    choices = ["int","string"],
                    help="the number of instances per operator")
parser.add_argument("--latency-file", default="latencies",
                    help="a prefix for the latency log files")
parser.add_argument("--throughput-file", default="throughputs",
                    help="a prefix for the rate log files")
parser.add_argument("--dump-file", default="",
                    help="a prefix for the chrome dump file")
parser.add_argument("--sample-period", default=100,
                    help="every how many input records latency is measured.")
parser.add_argument("--record-size", default=10,
                    help="the size of a record of type string in bytes")
parser.add_argument("--partitioning", default = "round_robin",
                    choices = ["shuffle","round_robin","broadcast"],
                    help="whether to shuffle or balance after each stage")
parser.add_argument("--source-rate", default=-1,
                    type=lambda x: ((float(x) == -1) or float(x)) or
                                parser.error("Source rate cannot be zero."),
                    help="source output rate (records/s)")
parser.add_argument("--warm-up", default=False,
                    action='store_true',
                    help="whether to use a first round of data to warmup")
# Queue-related parameters
parser.add_argument("--queue-size", default=100,
                    help="the queue size in number of batches")
parser.add_argument("--batch-size", default=1000,
                    help="the batch size in number of elements")
parser.add_argument("--flush-timeout", default=0.1,
                    help="the timeout to flush a batch")
parser.add_argument("--prefetch-depth", default=1,
                    help="the number of batches to prefetch from plasma")
parser.add_argument("--background-flush", default=False,
                    help="whether to flush in the backrgound or not")
parser.add_argument("--max-throughput", default="inf",
                    help="maximum read throughput (records/s)")

# A custom sink used to collect per-record latencies
# Latencies are kept in memory and they are retrieved
# by the driver script after the job is finished
# TODO (john): Custom sinks should inherit from a CustomSink class
class Sink(object):
    def __init__(self):
        self.state = []

    # Evicts next record
    def evict(self, record):
        self.state.append(record)

    # Closes the sink
    def close(self):
        pass

    # Returns sink's state
    def get_state(self):
        return self.state

def compute_elapsed_time(record):
    generation_time, _ = record
    if generation_time != -1:
        # TODO (john): Clock skew might distort elapsed time
        return [time.time() - generation_time]
    else:
        return []

def create_and_run_dataflow(num_nodes,  num_sources,
                            redis_shards, redis_max_memory,
                            plasma_memory, rounds, num_stages,
                            dataflow_parallelism, partitioning,
                            record_type, record_size,
                            queue_config, sample_period,
                            latency_filename, throughput_filename,
                            dump_filename, task_based, source_rate,
                            warm_up):

    assert num_stages >= 0, (num_stages)

    # Create streaming environment, construct and run dataflow
    env = Environment()
    env.set_queue_config(queue_config)
    env.set_parallelism(dataflow_parallelism)
    env.enable_logging()
    if task_based:
        env.enable_tasks()
    node_prefix = utils.CLUSTER_NODE_PREFIX
    stages_per_node = math.trunc(
                            math.ceil((num_stages + num_sources) / num_nodes))
    id = 0
    node_id = node_prefix + str(id)
    stream = env.source(utils.RecordGenerator(rounds, record_type,
                                    record_size, sample_period, source_rate,
                                    warm_up),
                                    name="source",
                                    placement=[node_id] * num_sources)
    # TODO (john): Use custom partitioning here to shuffle by key
    if partitioning == "shuffle":
        stream = stream.shuffle()
    for stage in range(num_stages):
        id = (stage + 1) // stages_per_node
        mapping = [node_prefix + str(id)] * dataflow_parallelism
        if stage < num_stages - 1:
            stream = stream.map(lambda record: record, name="map_"+str(stage),
                                placement=mapping)
        else: # Last stage actors should compute the per-record latencies
            stream = stream.flat_map(compute_elapsed_time, name="flatmap",
                                     placement=mapping)
    mapping = [node_prefix + str(id)] * dataflow_parallelism
    _ = stream.sink(Sink(), name="sink", placement=mapping)
    start = time.time()
    dataflow = env.execute()
    ray.get(dataflow.termination_status())

    # Write log files
    max_queue_size = queue_config.max_size
    max_batch_size = queue_config.max_batch_size
    batch_timeout = queue_config.max_batch_time
    prefetch_depth = queue_config.prefetch_depth
    background_flush = queue_config.background_flush
    input_rate = source_rate if source_rate > 0 else "inf"
    all = "-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}".format(
        num_nodes, input_rate,
        redis_shards, redis_max_memory, plasma_memory,
        rounds, sample_period,
        record_type, record_size,
        max_queue_size, max_batch_size, batch_timeout, prefetch_depth,
        background_flush, num_stages,
        partitioning, task_based, dataflow_parallelism
    )
    write_log_files(all, latency_filename,
                    throughput_filename, dump_filename, dataflow)
    logger.info("Elapsed time: {}".format(time.time()-start))

# Collects sampled latencies and throughputs from
# actors in the dataflow and writes the log files
def write_log_files(all_parameters, latency_filename,
                    throughput_filename,  dump_filename, dataflow):

    # Dump timeline
    if dump_filename:
        dump_filename = dump_filename + all_parameters
        ray.global_state.chrome_tracing_dump(dump_filename)

    # Collect sampled per-record latencies
    sink_id = dataflow.operator_id("sink")
    local_states = ray.get(dataflow.state_of(sink_id))
    latencies = [state for state in local_states if state is not None]
    latency_filename = latency_filename + all_parameters
    with open(latency_filename, "w") as tf:
        for _, latency_values in latencies:
            if latency_values is not None:
                for value in latency_values:
                    tf.write(str(value) + "\n")

    # Collect throughputs from all actors
    ids = dataflow.operator_ids()
    rates = []
    for id in ids:
        logs = ray.get(dataflow.logs_of(id))
        rates.extend(logs)
    throughput_filename = throughput_filename + all_parameters
    with open(throughput_filename, "w") as tf:
        for actor_id, in_rate, out_rate in rates:
            operator_id, instance_id = actor_id
            operator_name = dataflow.name_of(operator_id)
            for i, o in zip_longest(in_rate, out_rate, fillvalue=0):
                tf.write(
                    str("(" + str(operator_id) + ", " + str(
                     operator_name) + ", " + str(
                     instance_id)) + ")" + " | " + str(
                     i) + " | " + str(o) + "\n")

if __name__ == "__main__":
    args = parser.parse_args()

    rounds = int(args.rounds)
    num_nodes = int(args.nodes)
    num_redis_shards = int(args.redis_shards)
    redis_max_memory = int(args.redis_max_memory)
    plasma_memory = int(args.plasma_memory)
    task_based = args.task_based
    num_stages = int(args.num_stages)
    latency_filename = str(args.latency_file)
    throughput_filename = str(args.throughput_file)
    dump_filename = str(args.dump_file)
    sample_period = int(args.sample_period)
    dataflow_parallelism = int(args.dataflow_parallelism)
    record_type = str(args.record_type)
    record_size = int(args.record_size) if record_type == "string" else None
    partitioning = str(args.partitioning)
    max_queue_size = int(args.queue_size)
    max_batch_size = int(args.batch_size)
    batch_timeout = float(args.flush_timeout)
    prefetch_depth = int(args.prefetch_depth)
    background_flush = bool(args.background_flush)
    source_rate = float(args.source_rate)
    warm_up = bool(args.warm_up)
    pin_processes = bool(args.pin_processes)

    logger.info("== Parameters ==")
    logger.info("Rounds: {}".format(rounds))
    logger.info("Number of nodes: {}".format(num_nodes))
    logger.info("Number of Redis shards: {}".format(num_redis_shards))
    logger.info("Max memory per Redis shard: {}".format(redis_max_memory))
    logger.info("Plasma memory: {}".format(plasma_memory))
    logger.info("Number of stages: {}".format(num_stages))
    logger.info("Task-based execution: {}".format(task_based))
    logger.info("Sample period: {}".format(sample_period))
    logger.info("Latency file prefix: {}".format(latency_filename))
    logger.info("Throughput file prefix: {}".format(throughput_filename))
    logger.info("Dump file prefix: {}".format(dump_filename))
    logger.info("Parallelism: {}".format(dataflow_parallelism))
    logger.info("Record type: {}".format(record_type))
    if record_type == "string":
        logger.info("Record size: {}".format(record_size))
    logger.info("Partitioning: {}".format(partitioning))
    logger.info("Max queue size: {}".format(max_queue_size))
    logger.info("Max batch size: {}".format(max_batch_size))
    logger.info("Batch timeout: {}".format(batch_timeout))
    logger.info("Prefetch depth: {}".format(prefetch_depth))
    logger.info("Background flush: {}".format(background_flush))
    message = (" (as fast as it gets)") if source_rate < 0 else ""
    logger.info("Source rate: {}".format(source_rate) + message)
    logger.info("Warm_up: {}".format(warm_up))
    logger.info("Pin processes: {}".format(pin_processes))

    # Estimate the ideal output rate of a single source instance
    # when it is not backpressured by downstream operators in the chain
    source = utils.RecordGenerator(rounds, record_type, record_size,
                                   sample_period)
    start = time.time()
    records = source.drain()
    rate = records / (time.time() - start)
    logger.info("Ideal rate (per source instance): {}".format(rate))

    # This estimation makes sense only if source(s) live at the
    # same machine with this script (true for micro-benchmarks)
    num_sources = math.trunc(
                   math.ceil(source_rate / rate)) if rate < source_rate else 1

    # Start Ray with the specified configuration
    utils.start_ray(num_nodes, num_redis_shards, plasma_memory,
                    redis_max_memory, num_stages, dataflow_parallelism,
                    num_sources, pin_processes)

    # Use pickle for BatchedQueue
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    # In queue-based execution, all batched queues have the same configuration
    queue_config = QueueConfig(max_queue_size,
                        max_batch_size, batch_timeout,
                        prefetch_depth, background_flush)

    logger.info("== Testing Chaining ==")
    create_and_run_dataflow(num_nodes,  num_sources, num_redis_shards,
                            redis_max_memory,
                            plasma_memory, rounds, num_stages,
                            dataflow_parallelism, partitioning,
                            record_type, record_size,
                            queue_config, sample_period,
                            latency_filename, throughput_filename,
                            dump_filename, task_based, source_rate,
                            warm_up)

    utils.shutdown_ray(sleep=2)
