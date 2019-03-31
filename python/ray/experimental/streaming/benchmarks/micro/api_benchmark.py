from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
try:
    from itertools import zip_longest as zip_longest
except:
    from itertools import izip_longest as zip_longest
import logging
import numpy as np
import random
import string
import sys
import time

import ray
from ray.experimental.streaming.batched_queue import BatchedQueue
from ray.experimental.streaming.communication import QueueConfig
from ray.experimental.streaming.streaming import Environment

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

parser = argparse.ArgumentParser()
parser.add_argument("--rounds", default=10,
                    help="the number of experiment rounds")
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
parser.add_argument("--sample-period", default=1,
                    help="every how many input records latency is measured.")
parser.add_argument("--record-size", default=10,
                    help="the size of a record of type string in bytes")
parser.add_argument("--partitioning", default = "round_robin",
                    choices = ["shuffle","round_robin","broadcast"],
                    help="whether to shuffle or balance after each stage")
parser.add_argument("--max-source-rate", default="inf",
                    help="maximum source output rate (records/s)")
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
parser.add_argument("--prefetch-depth", default=10,
                    help="the number of batches to prefetch from plasma")
parser.add_argument("--background-flush", default=False,
                    help="whether to flush in the backrgound or not")
parser.add_argument("--max-throughput", default="inf",
                    help="maximum read throughput (records/s)")

# A custom source that periodically assigns timestamps to records
class Source(object):
    def __init__(self, rounds, record_type="int",
                 record_size=None, sample_period=1,
                 fixed_rate=float("inf"), warm_up=False):
        assert rounds > 0
        self.warm_up = warm_up
        if self.warm_up:
            rounds += 1
        self.total_elements = 100000 * rounds
        self.total_count = 0
        self.period = sample_period
        self.fixed_rate = fixed_rate
        self.rate_count = 0
        self.start = time.time()
        self.count = 0
        self.current_round = 0
        self.record_type = record_type
        self.record_size = record_size
        self.record = -1

        # Set the right function
        if self.record_type == "int":
            self.__get_next_record = self.__get_next_int
        else:
            self.__get_next_record = self.__get_next_string

    # Returns the next int
    def __get_next_int(self):
        self.record += 1
        return self.record

    # Returns the next random string
    def __get_next_string(self):
        return "".join(random.choice(
                string.ascii_letters + string.digits) for _ in range(
                                                        self.record_size))

    # Generates next record
    def get_next(self):
        if self.total_count == self.total_elements:
            return None
        record = self.__get_next_record()
        self.total_count += 1
        self.rate_count += 1
        # Measure source rate per round
        if self.rate_count == 100000:
            self.rate_count = 0
            self.start = time.time()
            time.sleep(0.001)
        while (self.rate_count / (time.time() - self.start) >
               self.fixed_rate):
            time.sleep(0.01)
        # Do a first round to warm up
        if self.warm_up and self.total_count <= 100000:
            if self.total_count == 100000:
                time.sleep(2)  # Wait a bit for the queues to drain
            return (-1,record)
        self.count += 1
        if self.count == self.period:
            self.count = 0
            return (time.time(),record)  # Assign the generation timestamp
        else:
            return (-1,record)

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

def create_and_run_dataflow(rounds, num_stages, dataflow_parallelism,
                            partitioning, record_type, record_size,
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
    stream = env.source(Source(rounds, record_type,
                                record_size, sample_period, source_rate,
                                warm_up),
                                name="source")
    if partitioning == "shuffle":
        stream = stream.shuffle()
    for stage in range(num_stages):
        if stage < num_stages - 1:
            stream = stream.map(lambda record: record, name="map_"+str(stage))
        else: # Last stage actors should compute the per-record latencies
            stream = stream.flat_map(compute_elapsed_time, name="flatmap")
    _ = stream.sink(Sink(), name="sink")
    start = time.time()
    dataflow = env.execute()
    ray.get(dataflow.termination_status())

    # Write log files
    max_queue_size = queue_config.max_size
    max_batch_size = queue_config.max_batch_size
    batch_timeout = queue_config.max_batch_time
    prefetch_depth = queue_config.prefetch_depth
    background_flush = queue_config.background_flush
    all_parameters = "-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}".format(
        rounds, sample_period,
        record_type, record_size,
        max_queue_size, max_batch_size, batch_timeout, prefetch_depth,
        background_flush, num_stages,
        partitioning, task_based, dataflow_parallelism
    )
    write_log_files(all_parameters, latency_filename,
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
    ray.init()
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    args = parser.parse_args()

    rounds = int(args.rounds)
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
    source_rate = float(args.max_source_rate)
    warm_up = bool(args.warm_up)

    logger.info("== Parameters ==")
    logger.info("Rounds: {}".format(rounds))
    logger.info("Number of stages: {}".format(num_stages))
    logger.info("Task-based execution: {}".format(task_based))
    logger.info("Sample period: {}".format(sample_period))
    logger.info("Latency file: {}".format(latency_filename))
    logger.info("Throughput file: {}".format(throughput_filename))
    logger.info("Dump file: {}".format(dump_filename))
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
    logger.info("Source rate: {}".format(source_rate))
    logger.info("Warm_up: {}".format(warm_up))

    # Estimate the ideal throughput
    source = Source(rounds, record_type, record_size,
                    sample_period, source_rate)
    count = 0
    start = time.time()
    while True:
        next = source.get_next()
        if next is None:
            break
        count += 1
    logger.info("Ideal throughput: {}".format(count / (time.time()-start)))

    queue_config = QueueConfig(max_queue_size,
                        max_batch_size, batch_timeout,
                        prefetch_depth, background_flush)

    logger.info("== Testing Chaining ==")
    create_and_run_dataflow(rounds, num_stages, dataflow_parallelism,
                            partitioning, record_type, record_size,
                            queue_config, sample_period,
                            latency_filename, throughput_filename,
                            dump_filename, task_based, source_rate,
                            warm_up)
    ray.shutdown()
