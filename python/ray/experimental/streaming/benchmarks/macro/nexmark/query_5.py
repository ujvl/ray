from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import math
import string
import sys
import time

import ray
import ray.experimental.streaming.benchmarks.utils as utils
import ray.experimental.streaming.benchmarks.macro.nexmark.data_generator as dg
from ray.experimental.streaming.batched_queue import BatchedQueue
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Bid
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Record
from ray.experimental.streaming.communication import QueueConfig
from ray.experimental.streaming.streaming import Environment

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

parser = argparse.ArgumentParser()
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
parser.add_argument("--bids-file", required=True,
                    help="Path to the bids file")
parser.add_argument("--enable-logging", default=False,
                    action='store_true',
                    help="whether to log actor latency and throughput")
parser.add_argument("--queue-based", default=False,
                    action='store_true',
                    help="queue-based execution")
parser.add_argument("--dataflow-parallelism", default=1,
                    help="the number of instances per operator")
parser.add_argument("--latency-file", default="latencies",
                    help="a prefix for the latency log files")
parser.add_argument("--throughput-file", default="throughputs",
                    help="a prefix for the rate log files")
parser.add_argument("--dump-file", default="",
                    help="a prefix for the chrome dump file")
parser.add_argument("--sample-period", default=100,
                    help="every how many input records latency is measured.")
parser.add_argument("--bids-rate", default=-1,
                    type=lambda x: float(x) or
                                parser.error("Source rate cannot be zero."),
                    help="source output rate (records/s)")
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

# Used to count number of bids per auction
class AggregationLogic(object):
    def __init__(self):
        pass

    # Initializes bid counter
    def initialize(self, bid):
        return (bid.auction, 1)

    # Updates number of bids per auction with the given record
    def update(self, old_state, bid):
        updated_state = None  # Tuples are immutable, so create a new one
        for i, state_object in enumerate(old_state):
            auction, old_value = state_object
            if auction == bid.auction:
                # logger.info("Old count: {}:{}".format(auction,old_value))
                updated_state = (auction, old_value+1)
                break
        old_state.pop(i)  # Remove old
        assert updated_state is not None
        # logger.info("New count: {}:{}".format(updated_state[0],
        #                                       updated_state[1]))
        old_state.append(updated_state)

if __name__ == "__main__":

    args = parser.parse_args()

    num_nodes = int(args.nodes)
    num_redis_shards = int(args.redis_shards)
    redis_max_memory = int(args.redis_max_memory)
    plasma_memory = int(args.plasma_memory)
    bids_file = str(args.bids_file)
    latency_filename = str(args.latency_file)
    throughput_filename = str(args.throughput_file)
    dump_filename = str(args.dump_file)
    logging = bool(args.enable_logging)
    sample_period = int(args.sample_period)
    task_based = not bool(args.queue_based)
    dataflow_parallelism = int(args.dataflow_parallelism)
    max_queue_size = int(args.queue_size)
    max_batch_size = int(args.batch_size)
    batch_timeout = float(args.flush_timeout)
    prefetch_depth = int(args.prefetch_depth)
    background_flush = bool(args.background_flush)
    bids_rate = float(args.bids_rate)
    pin_processes = bool(args.pin_processes)

    logger.info("== Parameters ==")
    logger.info("Number of nodes: {}".format(num_nodes))
    logger.info("Number of Redis shards: {}".format(num_redis_shards))
    logger.info("Max memory per Redis shard: {}".format(redis_max_memory))
    logger.info("Plasma memory: {}".format(plasma_memory))
    logger.info("Logging: {}".format(logging))
    logger.info("Sample period: {}".format(sample_period))
    logger.info("Task-based execution: {}".format(task_based))
    logger.info("Bids file: {}".format(bids_file))
    logger.info("Latency file prefix: {}".format(latency_filename))
    logger.info("Throughput file prefix: {}".format(throughput_filename))
    logger.info("Dump file prefix: {}".format(dump_filename))
    logger.info("Parallelism: {}".format(dataflow_parallelism))
    logger.info("Max queue size: {}".format(max_queue_size))
    logger.info("Max batch size: {}".format(max_batch_size))
    logger.info("Batch timeout: {}".format(batch_timeout))
    logger.info("Prefetch depth: {}".format(prefetch_depth))
    logger.info("Background flush: {}".format(background_flush))
    message = (" (as fast as it gets)") if bids_rate < 0 else ""
    logger.info("Bids rate: {}".format(bids_rate) + message)
    logger.info("Pin processes: {}".format(pin_processes))

    # Start Ray with the specified configuration
    utils.start_ray(num_nodes, num_redis_shards, plasma_memory,
                    redis_max_memory, 1, dataflow_parallelism,
                    1, pin_processes)

    # We just have a source stage followed by a sliding window
    stages_per_node = math.trunc(math.ceil(2 / num_nodes))

    # Use pickle for BatchedQueue
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    # Batched queue configuration
    queue_config = QueueConfig(max_queue_size,
                        max_batch_size, batch_timeout,
                        prefetch_depth, background_flush)

    # Create streaming environment, construct and run dataflow
    env = Environment()
    env.set_queue_config(queue_config)
    env.set_parallelism(dataflow_parallelism)
    if logging:
        env.enable_logging()
    if task_based:
        env.enable_tasks()

    watermark_interval = 1000  # 1s

    # Add the bids source
    bids_source = env.source(dg.NexmarkEventGenerator(bids_file,
                                "Bid",
                                bids_rate, sample_period),
                                watermark_interval=watermark_interval,
                                name="Bids Source",
                                placement=[utils.CLUSTER_NODE_PREFIX + "0"])
    bids = bids_source.partition(lambda bid: bid.auction)
    # Add the join
    id = 1 // stages_per_node
    mapping = [utils.CLUSTER_NODE_PREFIX + str(id)] * dataflow_parallelism
    # Add the filter
    output = bids.event_time_window(3600000, 60000,  # 60' window, 1' slide
                        # The custom aggregation logic (see above)
                        aggregation_logic=AggregationLogic(),
                        name="Cound Bids per Auction per Window",
                        placement=mapping)
    # Add a final custom sink to measure latency if logging is enabled
    output.sink(dg.LatencySink(), name="sink", placement=mapping)

    start = time.time()
    dataflow = env.execute()
    ray.get(dataflow.termination_status())

    # Write log files
    max_queue_size = queue_config.max_size
    max_batch_size = queue_config.max_batch_size
    batch_timeout = queue_config.max_batch_time
    prefetch_depth = queue_config.prefetch_depth
    background_flush = queue_config.background_flush
    bids_rate = bids_rate if bids_rate > 0 else "inf"
    all = "-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}".format(
        num_nodes, bids_rate,
        num_redis_shards, redis_max_memory, plasma_memory,
        sample_period, logging,
        max_queue_size, max_batch_size, batch_timeout, prefetch_depth,
        background_flush, pin_processes,
        task_based, dataflow_parallelism
    )
    utils.write_log_files(all, latency_filename,
                          throughput_filename, dump_filename, dataflow)

    logger.info("Elapsed time: {}".format(time.time() - start))

    utils.shutdown_ray(sleep=2)
