from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import math
import random
import string
import sys
import time

import ray
from ray.experimental.streaming.batched_queue import BatchedQueue
import ray.experimental.streaming.benchmarks.utils as utils

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
parser.add_argument("--num-stages", default=1,
                    help="the number of queues in the chain")
parser.add_argument("--record-type", default="int",
                    choices = ["int","string"],
                    help="the number of instances per operator")
parser.add_argument("--record-size", default=10,
                    help="the size of a record of type string in bytes")
parser.add_argument("--latency-file", default="latencies",
                    help="a prefix for the latency log files")
parser.add_argument("--throughput-file", default="throughputs",
                    help="a prefix for the rate log files")
parser.add_argument("--dump-file", default="",
                    help="a prefix for the chrome dump file")
parser.add_argument("--sample-period", default=100,
                    help="every how many input records latency is measured.")
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

@ray.remote
class Node(object):
    """An actor that reads from an input queue and writes to an output queue.

    Attributes:
        id (int): The id of the actor.
        queue (BatchedQueue): The input queue.
        out_queue (BatchedQueue): The output queue.
        max_reads_per_second (float): The max read throughput (default: inf).
        num_reads (int): Number of elements read.
        num_writes (int): Number of elements written.
    """
    def __init__(self, id, in_queue, out_queue, record_generator,
                    max_reads_per_second=float("inf"),log_latency=False):
        self.id = id
        self.queue = in_queue
        self.out_queue = out_queue
        self.max_reads_per_second = max_reads_per_second
        self.num_reads = 0
        self.num_writes = 0
        self.throughputs = []
        self.latencies = []
        self.start = time.time()
        self.log_latency = log_latency
        self.record_generator = record_generator

    def ping(self):
        return

    def read_write(self, rounds):
        # Start spinning
        debug_log = "[actor {}] Reads throttled to {} reads/s"
        log = ""
        if self.queue is None:
            log += "[actor {}] Writes per second {}"
            self.out_queue.enable_writes()
        elif self.out_queue is not None:
            self.out_queue.enable_writes()
            log += "[actor {}] Reads/Writes per second {}"
        else:   # It's just a reader
            log += "[actor {}] Reads per second {}"

        # It's the source
        if self.queue is None:
            start = time.time()
            counter = 0
            while True:
                record = self.record_generator.get_next()
                if record is None:
                    break
                self.out_queue.put_next(record)
                self.num_writes += 1
                counter += 1
                if counter == 100000:
                    self.throughputs.append(counter / (time.time() - start))
                    counter = 0
                    start = time.time()
            # Flush any remaining elements
            self.out_queue._flush_writes()
            return (self.id, self.latencies, self.throughputs)

        # It's a read-write or a sink node
        for _ in range(rounds):
            start = time.time()
            N = 100000
            for _ in range(N):
                record = self.queue.read_next()
                start_time, value = record
                self.num_reads += 1
                if self.out_queue is not None:  # It's a sink
                    self.out_queue.put_next((start_time,value))
                    self.num_writes += 1
                elif self.log_latency:  # Log end-to-end latency
                    # TODO (john): Clock skew might distort elapsed time
                    if start_time != -1:
                        elapsed_time = time.time() - start_time
                        self.latencies.append(elapsed_time)
                while (self.num_reads / (time.time() - self.start) >
                        self.max_reads_per_second):
                    time.sleep(0.1)
                    # logger.debug(debug_log.format(self.id,
                    #                     self.max_reads_per_second))
            self.throughputs.append(N / (time.time() - start))
            # logger.info(log.format(self.id,
            #                        N / (time.time() - start)))
        # Flush any remaining elements
        if self.out_queue is not None:
            self.out_queue._flush_writes()
        return (self.id, self.latencies, self.throughputs)

def benchmark_queue(num_nodes, source_rate,
                    redis_shards, redis_max_memory, plasma_memory,
                    rounds, latency_filename,
                    throughput_filename, dump_filename,
                    record_type, record_size,
                    sample_period, max_queue_size,
                    max_batch_size, batch_timeout,
                    prefetch_depth, background_flush,
                    num_stages, max_reads_per_second=float("inf"),
                    warm_up=False):

    assert num_stages >= 1
    assert num_nodes >= 1

    first_queue = BatchedQueue(
                    "ID",           # Dummy channel id
                    "SRC_OP_ID",    # Dummy source operator id
                    0,              # Dummy source instance id
                    "DST_OP_ID",    # Dummy destination operator id
                    0,              # Dummy destination instance id
                    max_size=max_queue_size,
                    max_batch_size=max_batch_size,
                    max_batch_time=batch_timeout,
                    prefetch_depth=prefetch_depth,
                    background_flush=background_flush)

    previous_queue = first_queue
    logs = []
    log_latency = False
    nodes = []
    node_prefix = utils.CLUSTER_NODE_PREFIX
    generator = utils.RecordGenerator(rounds,
               record_type=record_type,
               record_size=record_size,
               sample_period=sample_period,
               fixed_rate=source_rate,
               warm_up=warm_up)
    args = [-1, None, first_queue, generator, max_reads_per_second, log_latency]
    source = Node._remote(args=args, kwargs=None,
                         resources={node_prefix+"0": 1})
    nodes.append(source)
    stages_per_node = math.trunc(math.ceil(num_stages / num_nodes))
    for i in range(num_stages):
        # Construct the batched queue
        in_queue = previous_queue
        out_queue = None
        if i < num_stages-1:
            out_queue = BatchedQueue(
                "ID",           # Dummy channel id
                "SRC_OP_ID",    # Dummy source operator id
                0,              # Dummy source instance id
                "DST_OP_ID",    # Dummy destination operator id
                0,              # Dummy destination instance id
                max_size=max_queue_size,
                max_batch_size=max_batch_size,
                max_batch_time=batch_timeout,
                prefetch_depth=prefetch_depth,
                background_flush=background_flush)
        else:  # The last actor should log per-record latencies
            log_latency = True
        node_id = node_prefix
        node_id += str(i // stages_per_node)
        args = [i, in_queue, out_queue, None,
                max_reads_per_second, log_latency]
        node = Node._remote(args=args, kwargs=None, resources={node_id: 1})
        nodes.append(node)
        previous_queue = out_queue

    # Wait for all of the node actors to come up.
    ray.get([node.ping.remote() for node in nodes])
    for node in nodes:
        # Each actor returns a list of per-record latencies
        # sampled every 'sample_period' records
        # and a list of throughput values estimated for every
        # N = 100000 records in its input
        logs.append(node.read_write.remote(rounds))

    # Collect logs from all actors
    result = ray.get(logs)

    input_rate = source_rate if source_rate > 0 else "inf"

    # Write log files
    all = "-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}".format(
        num_nodes, input_rate,
        redis_shards, redis_max_memory, plasma_memory,
        rounds, sample_period,
        record_type, record_size,
        max_queue_size, max_batch_size, batch_timeout, prefetch_depth,
        background_flush, num_stages, max_reads_per_second
    )

    # Dump timeline
    if dump_filename:
        dump_filename = dump_filename + all
        ray.global_state.chrome_tracing_dump(dump_filename)

    # Write sampled per-record latencies
    latency_filename = latency_filename + all
    with open(latency_filename, "w") as lf:
            for node_id, latencies, _ in result:
                for latency in latencies:
                    lf.write(str(latency) + "\n")

    # Collect throughputs from all actors
    throughput_filename = throughput_filename + all
    with open(throughput_filename, "w") as tf:
            for node_id, _, throughputs in result:
                for throughput in throughputs:
                    tf.write(str(node_id) + " | " + str(throughput) + "\n")


if __name__ == "__main__":
    # Benchmark parameters
    args = parser.parse_args()

    rounds = int(args.rounds)
    num_nodes = int(args.nodes)
    num_redis_shards = int(args.redis_shards)
    redis_max_memory = int(args.redis_max_memory)
    plasma_memory = int(args.plasma_memory)
    latency_filename = str(args.latency_file)
    throughput_filename = str(args.throughput_file)
    dump_filename = str(args.dump_file)
    sample_period = int(args.sample_period)
    record_type = str(args.record_type)
    record_size = int(args.record_size) if record_type == "string" else None
    max_queue_size = int(args.queue_size)
    max_batch_size = int(args.batch_size)
    batch_timeout = float(args.flush_timeout)
    prefetch_depth = int(args.prefetch_depth)
    background_flush = bool(args.background_flush)
    num_stages = int(args.num_stages)
    max_reads_per_second = float(args.max_throughput)
    source_rate = float(args.source_rate)
    warm_up = bool(args.warm_up)
    pin_processes = bool(args.pin_processes)

    logger.info("== Parameters ==")
    logger.info("Rounds: {}".format(rounds))
    logger.info("Number of queues: {}".format(num_stages))
    logger.info("Sample period: {}".format(sample_period))
    logger.info("Latency file prefix: {}".format(latency_filename))
    logger.info("Throughput file prefix: {}".format(throughput_filename))
    logger.info("Dump file prefix: {}".format(dump_filename))
    logger.info("Record type: {}".format(record_type))
    if record_type == "string":
        logger.info("Record size: {}".format(record_size))
    logger.info("Max queue size: {}".format(max_queue_size))
    logger.info("Max batch size: {}".format(max_batch_size))
    logger.info("Batch timeout: {}".format(batch_timeout))
    logger.info("Prefetch depth: {}".format(prefetch_depth))
    logger.info("Background flush: {}".format(background_flush))
    logger.info("Max read throughput: {}".format(max_reads_per_second))
    message = (" (as fast as it gets)") if source_rate < 0 else ""
    logger.info("Source rate: {}".format(source_rate) + message)
    logger.info("Warm_up: {}".format(warm_up))

    # Measure the throughput of the record generator when it is not
    # backpressured by downstream nodes in the chain
    generator = utils.RecordGenerator(rounds, record_type,
                                      record_size, sample_period)

    start = time.time()
    records = generator.drain()
    throughput = records / (time.time() - start)
    logger.info("Ideal throughput: {}".format(throughput))

    if source_rate > throughput:
        message = "Cannot reach source rate {} with a single source."
        logger.info(message.format(source_rate))
        sys.exit()

    # Start Ray with the specified configuration
    utils.start_ray(num_nodes, num_redis_shards, plasma_memory,
                    redis_max_memory, num_stages, 1, 1, pin_processes)

    # Use pickle for BatchedQueue
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    logger.info("== Testing Batched Queue Chaining ==")
    start = time.time()
    benchmark_queue(num_nodes, source_rate, num_redis_shards,
                    redis_max_memory, plasma_memory, rounds,
                    latency_filename, throughput_filename, dump_filename,
                    record_type, record_size,
                    sample_period, max_queue_size,
                    max_batch_size, batch_timeout,
                    prefetch_depth, background_flush,
                    num_stages,
                    max_reads_per_second, warm_up)
    logger.info("Elapsed time: {}".format(time.time()-start))

    utils.shutdown_ray(sleep=2)
