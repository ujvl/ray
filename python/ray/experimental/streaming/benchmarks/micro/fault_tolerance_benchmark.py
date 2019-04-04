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
import json

import ray
from ray.experimental.streaming.batched_queue import BatchedQueue
import ray.experimental.streaming.benchmarks.utils as utils
from ray.experimental.streaming.communication import QueueConfig
from ray.experimental.streaming.streaming import Environment


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

parser = argparse.ArgumentParser()
parser.add_argument("--rounds", default=5,
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
                    type=lambda x: float(x) or
                                parser.error("Source rate cannot be zero."),
                    help="source output rate (records/s)")
parser.add_argument("--sources", default=3,
                    type=lambda x: float(x) or
                                parser.error("At least one source needed."),
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
    def __init__(self, source_keys):
        self.records = {
                key: 0 for key in source_keys
                }

    # Evicts next record
    def evict(self, record):
        _, record = record
        source_key, val = record
        assert self.records[source_key] == val, (source_key, self.records[source_key], val)
        self.records[source_key] += 1

    # Closes the sink
    def close(self):
        pass

    # Returns sink's state
    def get_state(self):
        return self.records

def compute_elapsed_time(record):
    return [record]

def create_and_run_dataflow(args, num_nodes,  num_sources,
                            redis_shards, redis_max_memory,
                            plasma_memory, rounds, num_stages,
                            dataflow_parallelism, partitioning,
                            record_type, record_size,
                            queue_config, sample_period,
                            latency_filename, throughput_filename,
                            dump_filenamex, source_rate,
                            warm_up, cluster, internal_config):

    assert num_stages >= 0, (num_stages)

    # Create streaming environment, construct and run dataflow
    env = Environment()
    env.set_queue_config(queue_config)
    env.set_parallelism(dataflow_parallelism)
    env.enable_logging()
    env.enable_tasks()
    node_prefix = utils.CLUSTER_NODE_PREFIX
    stages_per_node = math.trunc(
                            math.ceil((num_stages + num_sources) / num_nodes))
    node_index = 0
    node_id = node_prefix + str(node_index)
    source_streams = []

    operator_ids = list(string.ascii_uppercase)
    source_keys = [operator_ids.pop(0) for _ in range(num_sources)]
    intermediate_keys = [operator_ids.pop(0) for _ in range(num_stages - 1)]
    # One sink.
    sink_key = operator_ids.pop(0)

    for i in range(num_sources):
        stream = env.source(utils.RecordGenerator(rounds, record_type,
                                    record_size, sample_period, source_rate,
                                    warm_up, key=source_keys[i]),
                                    name="source_" + str(i),
                                    placement=[node_id])
        print("source", node_id)
        # TODO (john): Use custom partitioning here to shuffle by key
        if partitioning == "shuffle":
            stream = stream.shuffle()
        source_streams.append(stream)
    stream = source_streams.pop()
    node_index += 1
    mapping = [node_prefix + str(node_index)] * dataflow_parallelism
    print("union", mapping)
    stream = stream.union(source_streams, name="union",
                          placement=mapping)
    for stage in range(1,num_stages):
        node_index += 1
        mapping = [node_prefix + str(node_index)] * dataflow_parallelism
        if stage < num_stages - 1:
            stream = stream.map(lambda record: record, name="map_"+str(stage),
                                placement=mapping)
        else: # Last stage actors should compute the per-record latencies
            stream = stream.flat_map(compute_elapsed_time, name="flatmap",
                                     placement=mapping)
        if partitioning == "shuffle":
            stream = stream.shuffle()
    mapping = [node_prefix + str(node_index)] * dataflow_parallelism
    _ = stream.sink(Sink(source_keys), name="sink", placement=mapping)
    start = time.time()
    dataflow = env.execute()

    nodes = cluster.list_all_nodes()
    node_resources = ["Node{}".format(i) for i in range(num_nodes)]
    intermediate_nodes = nodes[1:-1]
    intermediate_resources = node_resources[1:-1]
    time.sleep(5)
    print("intermediate nodes", intermediate_resources)
    # Kill and restart all intermediate operators.
    node_kwargs = {
        "num_cpus": 4,
        "object_store_memory": 10**9,
        "_internal_config": internal_config,
    }
    for node in intermediate_nodes:
        cluster.remove_node(node)
    for resource in intermediate_resources:
        node_kwargs["resources"] = {resource: 100}
        cluster.add_node(**node_kwargs)

    ray.get(dataflow.termination_status())
    print(ray.get(dataflow.state_of("sink")))

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
        partitioning, "True", dataflow_parallelism
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
    return

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
    num_redis_shards = int(args.redis_shards)
    redis_max_memory = int(args.redis_max_memory)
    plasma_memory = int(args.plasma_memory)
    num_stages = int(args.num_stages)
    if num_stages < 2:
        parser.error("Must have at least 2 stages (Source->Union->Flatmap).")
        sys.exit()
    num_nodes = num_stages + 1
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
    num_sources = int(args.sources)
    warm_up = bool(args.warm_up)
    pin_processes = bool(args.pin_processes)

    logger.info("== Parameters ==")
    logger.info("Rounds: {}".format(rounds))
    logger.info("Number of nodes: {}".format(num_nodes))
    logger.info("Number of Redis shards: {}".format(num_redis_shards))
    logger.info("Max memory per Redis shard: {}".format(redis_max_memory))
    logger.info("Plasma memory: {}".format(plasma_memory))
    logger.info("Number of stages: {}".format(num_stages))
    logger.info("Task-based execution: True")
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
    logger.info("Number of sources: {}".format(num_sources))
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
    if source_rate > rate:
        message = "Maximum rate per source is {} < {}"
        logger.info(message.format(rate, source_rate))
        sys.exit()

    # Start ray
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 20,
        "object_manager_repeated_push_delay_ms": 1000,
        "object_manager_pull_timeout_ms": 1000,
        "gcs_delay_ms": 100,
        # We will kill all nondeterministic workers, so make sure we can
        # tolerate that many failures.
        "lineage_stash_max_failures": num_stages,
        "node_manager_forward_task_retry_timeout_milliseconds": 100,
    })

    # Start Ray with the specified configuration
    print("NUM NODES", num_nodes)
    cluster = utils.start_ray(num_nodes, num_redis_shards, plasma_memory,
                    redis_max_memory, num_stages, dataflow_parallelism,
                    num_sources, pin_processes, internal_config)

    # Use pickle for BatchedQueue
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    # In queue-based execution, all batched queues have the same configuration
    queue_config = QueueConfig(max_queue_size,
                        max_batch_size, batch_timeout,
                        prefetch_depth, background_flush)

    logger.info("== Testing Lineage Stash ==")
    create_and_run_dataflow(args, num_nodes,  num_sources, num_redis_shards,
                            redis_max_memory,
                            plasma_memory, rounds, num_stages,
                            dataflow_parallelism, partitioning,
                            record_type, record_size,
                            queue_config, sample_period,
                            latency_filename, throughput_filename,
                            dump_filename, source_rate,
                            warm_up, cluster, internal_config)

    utils.shutdown_ray(sleep=2)
