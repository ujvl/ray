from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import hashlib
import logging
import math
import time
import wikipedia

import ray.experimental.streaming.benchmarks.utils as utils

import ray
from ray.experimental.streaming.streaming import Environment
from ray.experimental.streaming.batched_queue import BatchedQueue
from ray.experimental.streaming.operator import OpType, PStrategy
from ray.experimental.streaming.communication import QueueConfig
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Watermark


logger = logging.getLogger(__name__)
logger.setLevel("INFO")

parser = argparse.ArgumentParser()
parser.add_argument("--pin-processes", default=False,
                    action='store_true',
                    help="whether to pin python processes to cores or not")
parser.add_argument("--simulate-cluster", default=False,
                    action='store_true',
                    help="simulate a Ray cluster on a single machine")
parser.add_argument("--fetch-data", default=False,
                    action='store_true',
                    help="fecth data from S3")
parser.add_argument("--omit-extra", default=False,
                    action='store_true',
                    help="omit extra field from events")
parser.add_argument("--records", default=-1,
                help="maximum number of records to replay from each source.")
parser.add_argument("--nodes", default=1,
                    help="total number of nodes in the cluster")
parser.add_argument("--redis-shards", default=1,
                    help="total number of Redis shards")
parser.add_argument("--redis-max-memory", default=10**9,
                    help="max amount of memory per Redis shard")
parser.add_argument("--plasma-memory", default=10**9,
                    help="amount of memory to start plasma with")
# Dataflow-related parameters
parser.add_argument("--placement-file", default="",
            help="Path to the file containing the explicit actor placement")
parser.add_argument("--words-file", required=True,
                    help="Path to the words file")
parser.add_argument("--enable-logging", default=False,
                    action='store_true',
                    help="whether to log actor latency and throughput")
parser.add_argument("--queue-based", default=False,
                    action='store_true',
                    help="queue-based execution")
parser.add_argument("--splitter-instances", default=1,
                    help="the number of splitter instances")
parser.add_argument("--reducer-instances", default=1,
                    help="the number of reduceer instances")
parser.add_argument("--latency-file", default="latencies",
                    help="a prefix for the latency log files")
parser.add_argument("--throughput-file", default="throughputs",
                    help="a prefix for the rate log files")
parser.add_argument("--dump-file", default="",
                    help="a prefix for the chrome dump file")
parser.add_argument("--sample-period", default=100,
                    help="every how many input records latency is measured.")
parser.add_argument("--source-rate", default=-1,
                    type=lambda x: float(x) or
                                parser.error("Source rate cannot be zero."),
                    help="source output rate (records/s)")
parser.add_argument("--sources", default=1,
                    # TODO (john): Add check
                    help="number of bid sources")
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



# A custom sink used to measure processing latency
class LatencySink(object):
    def __init__(self):
        self.state = []

    # Evicts next record
    def evict(self, record):
        generation_time, _, _ = record
        if generation_time is not None:
            # TODO (john): Clock skew might distort elapsed time
            self.state.append(time.time() - generation_time)

    # Closes the sink
    def close(self):
        pass

    # Returns sink's state
    def get_state(self):
        return self.state


# A custom data source that reads articles from wikipedia
# Custom data sources need to implement a get_next() method
# that returns the next data element, in this case sentences
class WordSource(object):
    def __init__(self, words_file, period=1000):
        # Titles in this file will be as queries
        self.words_file = words_file
        # TODO (john): Handle possible exception here
        self.word_reader = None
        self.done = False
        self.count = 0
        self.period = period

    def init(self,):
        self.lines = iter(list(open(self.words_file, "r").readlines()))
        self.word_reader = iter(self.lines)

    # Returns next sentence from the input file
    def get_next(self):
        if self.done:
            return None     # Source exhausted
        # Try next sentence
        try:
            sentence = next(self.lines)
        except StopIteration:
            self.done = True
            return None
        self.count += 1
        result = None
        if self.count == self.period:
            result = (time.time(), sentence)
            self.count = 0
        else:
            result = (-1, sentence)
        logger.info("Next sentence: {}".format(sentence))
        return result

# Splits input line into words and
# outputs records of the form (word,1)
def splitter(line):
    records = []
    words = line[1].split()
    for w in words:
        records.append((line[0], w, 1))  # line[0] is the timestamp (or -1)
    return records

# Returns the first attribute of a tuple
def key_selector(tuple):
    return tuple[1]

# Returns the second attribute of a tuple
def attribute_selector(tuple):
    return tuple[2]

def partition_fn(record):
    _time, word, _count = record
    return int(hashlib.sha1(word.encode("utf-8")).hexdigest(), 16)

if __name__ == "__main__":
    # Get program parameters
    args = parser.parse_args()

    num_nodes = int(args.nodes)
    simulate_cluster = bool(args.simulate_cluster)
    max_records = int(args.records)
    placement_file = str(args.placement_file)
    num_redis_shards = int(args.redis_shards)
    redis_max_memory = int(args.redis_max_memory)
    plasma_memory = int(args.plasma_memory)
    words_file = str(args.words_file)
    latency_filename = str(args.latency_file)
    throughput_filename = str(args.throughput_file)
    dump_filename = str(args.dump_file)
    logging = bool(args.enable_logging)
    sample_period = int(args.sample_period)
    task_based = not bool(args.queue_based)
    splitter_instances = int(args.splitter_instances)
    reducer_instances = int(args.reducer_instances)
    max_queue_size = int(args.queue_size)
    max_batch_size = int(args.batch_size)
    batch_timeout = float(args.flush_timeout)
    prefetch_depth = int(args.prefetch_depth)
    background_flush = bool(args.background_flush)
    source_rate = float(args.source_rate)
    num_sources = int(args.sources)
    pin_processes = bool(args.pin_processes)

    logger.info("== Parameters ==")
    logger.info("Number of nodes: {}".format(num_nodes))
    logger.info("Simulate cluster: {}".format(simulate_cluster))
    logger.info("Maximum number of records: {}".format(max_records))
    logger.info("Placement file: {}".format(placement_file))
    logger.info("Number of Redis shards: {}".format(num_redis_shards))
    logger.info("Max memory per Redis shard: {}".format(redis_max_memory))
    logger.info("Plasma memory: {}".format(plasma_memory))
    logger.info("Logging: {}".format(logging))
    logger.info("Sample period: {}".format(sample_period))
    logger.info("Task-based execution: {}".format(task_based))
    logger.info("Words file: {}".format(words_file))
    logger.info("Latency file prefix: {}".format(latency_filename))
    logger.info("Throughput file prefix: {}".format(throughput_filename))
    logger.info("Dump file prefix: {}".format(dump_filename))
    logger.info("Splitter instances: {}".format(splitter_instances))
    logger.info("Reducer instances: {}".format(reducer_instances))
    logger.info("Max queue size: {}".format(max_queue_size))
    logger.info("Max batch size: {}".format(max_batch_size))
    logger.info("Batch timeout: {}".format(batch_timeout))
    logger.info("Prefetch depth: {}".format(prefetch_depth))
    logger.info("Background flush: {}".format(background_flush))
    logger.info("Number of sources: {}".format(num_sources))
    message = (" (as fast as it gets)") if source_rate < 0 else ""
    logger.info("Source rate: {}".format(source_rate) + message)
    logger.info("Pin processes: {}".format(pin_processes))

    stage_parallelism = [splitter_instances, reducer_instances,
                         reducer_instances]
    placement = {}
    if simulate_cluster:  # Simulate a cluster with the given configuration
        utils.start_virtual_cluster(num_nodes, num_redis_shards,
                                    plasma_memory, redis_max_memory,
                                    stage_parallelism, num_sources,
                                    pin_processes)
        num_stages = 3  # We have a source followed by a split and a reduce
        stages_per_node = math.trunc(math.ceil(num_stages / num_nodes))
        source_node_id  = utils.CLUSTER_NODE_PREFIX + "0"
        placement["Words Source"] = [source_node_id] * num_sources
        id = 1 // stages_per_node
        splitter_node_id = utils.CLUSTER_NODE_PREFIX + str(id)
        placement["Splitter"] = [splitter_node_id] * splitter_instances
        id = 2 // stages_per_node
        reducer_node_id = utils.CLUSTER_NODE_PREFIX + str(id)
        placement["Sum"] = [reducer_node_id] * reducer_instances
        placement["sink"] = [reducer_node_id] * reducer_instances
    else:  # Connect to existing cluster
        if pin_processes:
            utils.pin_processes()
        ray.init(redis_address="localhost:6379")
        if not placement_file:
            sys.exit("No actor placement specified.")
        node_ids = utils.get_cluster_node_ids()
        logger.info("Found {} cluster nodes.".format(len(node_ids)))
        placement = utils.parse_placement(placement_file, node_ids)

    # Use pickle for BatchedQueue
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    # Batched queue configuration
    queue_config = QueueConfig(max_queue_size,
                        max_batch_size, batch_timeout,
                        prefetch_depth, background_flush)

    # Create streaming environment, construct and run dataflow
    env = Environment()
    env.set_queue_config(queue_config)
    if logging:
        env.enable_logging()
    if task_based:
        env.enable_tasks()

    # The following dataflow is a simple streaming wordcount
    #  with a rolling sum operator.
    # It reads articles from wikipedia, splits them in words,
    # shuffles words, and counts the occurences of each word.

    source_objects = [WordSource(words_file,
                                 sample_period) for _ in range(num_sources)]

    source_output = env.source(source_objects,
                                name="Words Source",
                                placement=placement[
                                "Words Source"]).set_parallelism(num_sources)

    words = source_output.flat_map(splitter,
                                   name="Splitter",
                                   placement=placement[
                                        "Splitter"]).set_parallelism(
                                                        splitter_instances)
    words = words.partition(partition_fn)

    output = words.sum(attribute_selector,
                       name="Sum",
                       placement=placement[
                                "Sum"]).set_parallelism(reducer_instances)

    output.sink(LatencySink(),
                name="sink",
                placement=placement["sink"]).set_parallelism(
                                                    reducer_instances)

    start = time.time()
    dataflow = env.execute()        # Deploys and executes the dataflow
    # Stay alive until execution finishes
    ray.get(dataflow.termination_status())
    end = time.time()

    # Write log files
    max_queue_size = queue_config.max_size
    max_batch_size = queue_config.max_batch_size
    batch_timeout = queue_config.max_batch_time
    prefetch_depth = queue_config.prefetch_depth
    background_flush = queue_config.background_flush
    source_rate = source_rate if source_rate > 0 else "inf"
    all = "-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}".format(
        num_nodes, source_rate, num_sources,
        num_redis_shards, redis_max_memory, plasma_memory,
        sample_period, logging,
        max_queue_size, max_batch_size, batch_timeout, prefetch_depth,
        background_flush, pin_processes,
        task_based, splitter_instances, reducer_instances
    )
    utils.write_log_files(all, latency_filename,
                          throughput_filename, dump_filename, dataflow)

    logger.info("Elapsed time: {}".format(time.time() - start))

    utils.shutdown_ray(sleep=2)

    logger.info("Elapsed time: {} secs".format(end - start))
