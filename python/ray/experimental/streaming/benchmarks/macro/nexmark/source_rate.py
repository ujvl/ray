from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import boto3
import logging
from statistics import mean
import time

import ray
import ray.experimental.streaming.benchmarks.macro.nexmark.data_generator as dg
import ray.experimental.streaming.benchmarks.utils as utils

from ray.experimental.streaming.batched_queue import BatchedQueue
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Auction
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Bid
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Person
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Record
from ray.experimental.streaming.communication import QueueConfig
from ray.experimental.streaming.streaming import Environment

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

parser = argparse.ArgumentParser()

parser.add_argument("--input-file", required=True,
                    help="path to the event file")
parser.add_argument("--queue-based", default=False,
                    action='store_true',
                    help="queue-based execution")
parser.add_argument("--source-type", default="auction",
                    choices=["auction","bid","person"],
                    help="source type")
parser.add_argument("--sample-period", default=1000,
                    help="every how many input records latency is measured.")

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

if __name__ == "__main__":

    args = parser.parse_args()

    in_file = str(args.input_file)
    task_based = not bool(args.queue_based)
    source_type = str(args.source_type)
    sample_period = int(args.sample_period)
    max_queue_size = int(args.queue_size)
    max_batch_size = int(args.batch_size)
    batch_timeout = float(args.flush_timeout)
    prefetch_depth = int(args.prefetch_depth)
    background_flush = bool(args.background_flush)

    logger.info("== Parameters ==")
    logger.info("Source type: {}".format(source_type))
    logger.info("Task-based execution: {}".format(task_based))
    logger.info("Input file: {}".format(in_file))
    logger.info("Latency sample period: {}".format(sample_period))
    logger.info("Max queue size: {}".format(max_queue_size))
    logger.info("Max batch size: {}".format(max_batch_size))
    logger.info("Batch timeout: {}".format(batch_timeout))
    logger.info("Prefetch depth: {}".format(prefetch_depth))
    logger.info("Background flush: {}".format(background_flush))


    s3 = boto3.resource('s3')
    s3.meta.client.download_file('nexmarkx', 'bids', 'bids.data')
    # Run program at the head node
    # redis_address="localhost:6379",
    ray.init(redis_address="localhost:6379")  # For source, sink, and progress monitor

    # Use pickle for BatchedQueue
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    # Batched queue configuration
    queue_config = QueueConfig(max_queue_size,
                        max_batch_size, batch_timeout,
                        prefetch_depth, background_flush)

    # Create streaming environment, construct and run dataflow
    env = Environment()
    env.set_queue_config(queue_config)
    env.enable_logging()  # Enable logging to measure throughput
    if task_based:
        env.enable_tasks()

    source = None

    if source_type == "auction":  # Add the auction source
        source = env.source(dg.NexmarkEventGenerator(in_file, "Auction", -1,
                                                     sample_period),
                            name="auction",
                            placement=["Node_0"])
    elif source_type ==  "bid":  # Add the bid source
        source = env.source(dg.NexmarkEventGenerator(in_file, "Bid", -1,
                                                     sample_period),
                                    name="bid",
                                    placement=["Node_0"])
    else:  # Add the person source
        assert source_type == "person"
        source = env.source(dg.NexmarkEventGenerator(in_file, "Person", -1,
                                                     sample_period),
                            name="person",
                            placement=["Node_0"])
    assert source is not None

    # Connect a dummy sink to measure latency as well
    _ = source.sink(dg.LatencySink(), name="sink", placement=["Node_0"])

    start = time.time()
    dataflow = env.execute()
    ray.get(dataflow.termination_status())  # Wait until source is exhausted

    # Collect throughputs from source
    raw_rates = ray.get(dataflow.logs_of(source_type))
    rates = [rate for _, _, out_rates in raw_rates for rate in out_rates]
    raw_latencies = ray.get(dataflow.state_of("sink"))
    latencies = [l for _, latencies in raw_latencies for l in latencies]
    if len(rates) > 0:
        logger.info("Mean source rate: {}".format(mean(rates)))
    else:
        logger.info("No rates found (maybe logging is off?)")
    if len(latencies) > 0:
        logger.info("Mean latency: {}".format(mean(latencies)))
    else:
        logger.info(
            "No latencies found (maybe the sample period is too large?)")

    logger.info("Elapsed time: {}".format(time.time() - start))

    utils.shutdown_ray(sleep=2)
