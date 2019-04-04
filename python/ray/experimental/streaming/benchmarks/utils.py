from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import math
import multiprocessing
import subprocess
import sys
import time

import ray
from ray.tests.cluster_utils import Cluster

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

CLUSTER_NODE_PREFIX = "Node_"

# Uses Linux's taskset command to pin each Python process to a CPU core
# Make sure that all python processes are up and running before calling this
def pin_processes():
    # Pins each python process to a specific core
    cmd_pids = ["pgrep", "python"]
    result = subprocess.check_output(cmd_pids)
    pids = [pid for pid in str(result.decode("ascii").strip()).split("\n")]
    print("Found {} python processes with PIDs: {}".format(len(pids), pids))
    cmd_pin = ["taskset", "-p", None, None]
    for i, pid in enumerate(pids):
        cmd_pin[2] = str(hex(i+1))  # Affinity mask
        cmd_pin[3] = pid
        subprocess.call(cmd_pin)

# Returns all node ids in a Ray cluster
def get_cluster_node_ids():
    node_ids = []
    for node in ray.global_state.client_table():
        for node_id in node["Resources"].keys():
            if "CPU" not in node_id and "GPU" not in node_id:
                node_ids.append(node_id)
    return node_ids

# Starts Ray with the given configuration.
# Assumes a chain dataflow where all stages have the same level of parallelism
# except sources, which can be configured arbitrarily to meet rate targets.
# Actor placement is done based on a N:1 mapping from dataflow stages to
# cluster nodes, i.e. a node might host more than one stages, but all operator
# instances of a particular stage will run at the same node
def start_ray(num_nodes, num_redis_shards, plasma_memory,
              redis_max_memory, num_stages, dataflow_parallelism,
              num_sources, pin, internal_config, api=True):
    # Simulate a cluster on a single machine
    cluster = Cluster()
    # 'num_stages' is the user-defined parameter that does not include sources
    # and sinks. We also need to count the actor for tracking progress
    num_actors = num_sources + dataflow_parallelism * (num_stages + 1) + 1
    if not api:  # No sink and progress monitoring when plain queues are used
        num_actors -= 2
    logger.info("Total number of required actors: {}".format(num_actors))
    num_cpus = multiprocessing.cpu_count()
    if num_cpus < num_actors:
        part_1 = "Dataflow contains {} actors".format(num_actors)
        part_2 = "but only {} available CPUs were found.".format(num_cpus)
        logger.error(part_1 + " " + part_2)
        # sys.exit()
    # The 'actors_per_stage' list includes only source and map instances
    actors_per_stage = [num_sources]
    actors_per_stage.extend([dataflow_parallelism for _ in range(num_stages)])
    stages_per_node = math.trunc(math.ceil(len(actors_per_stage) / num_nodes))
    message = "Number of stages per node: {} (source stage included)"
    logger.info(message.format(stages_per_node))
    assigned_actors = 0
    # The monitoring actor runs at the first node
    node_actors = 1 if api else 0
    for i in range(num_nodes):
        remaining_actors = num_actors - assigned_actors
        if remaining_actors == 0:  # No more nodes are needed
            break
        low = i * stages_per_node
        high = (i + 1) * stages_per_node
        if high >= len(actors_per_stage):  # Last node
            # Sinks run at the last node
            node_actors += dataflow_parallelism if api else 0
            high = len(actors_per_stage)
        node_actors += sum(n for n in actors_per_stage[low:high])
        # Add cluster node
        cluster.add_node(
            # Start only one Redis instance
            num_redis_shards=num_redis_shards if i == 0 else None,
            num_cpus=node_actors,
            num_gpus=0,
            resources={CLUSTER_NODE_PREFIX + str(i): node_actors},
            object_store_memory=plasma_memory,
            redis_max_memory=redis_max_memory,
            _internal_config=internal_config)
        assigned_actors += node_actors
        logger.info("Added node {} with {} CPUs".format(i, node_actors))
        node_actors = 0

    ray.init(redis_address=cluster.redis_address)

    if pin:  # Pin python processes to CPU cores (Linux only)
        logger.info("Waiting for python processes to come up...")
        time.sleep(5)  # Wait a bit for Ray to start
        pin_processes()
    return cluster

# Shuts down Ray and (optionally) sleeps for a given number of seconds
def shutdown_ray(sleep=0):
    ray.shutdown()
    time.sleep(sleep)

# A record generator used in bechmarks
# The generator periodically assigns timestamps to records so that we can
# measure end-to-end per-record latency
class RecordGenerator(object):
    """Generates records of type int or str.

    Attributes:
        rounds (int): Number of rounds, each one generating 100K records
        record_type (str): The type of records to generate ("int" or "string")
        record_size (int): The size of string in case record_type="string"
        sample_period (int): The period to measure record latency
                             (every 'sample_period' records)
        fixed_rate (int): The source rate (unbounded by default)
        warm_up (bool): Whether to do a first warm-up round or not
    """
    def __init__(self, rounds, record_type="int",
                 record_size=None, sample_period=1,
                 fixed_rate=-1,
                 warm_up=False,
                 key=None):

        assert rounds > 0, rounds
        assert fixed_rate != 0, fixed_rate

        self.warm_up = warm_up
        if self.warm_up:
            rounds += 1
        self.total_elements = 100000 * rounds
        self.total_count = 0
        self.period = sample_period
        self.fixed_rate = fixed_rate if fixed_rate > 0 else float("inf")
        self.rate_count = 0
        self.start = time.time()
        self.count = 0
        self.current_round = 0
        self.record_type = record_type
        self.record_size = record_size
        self.record = -1
        self.key = key
        # Set the right function
        if self.key is not None:
            self.__get_next_record = self.__get_next_keyed_int
        elif self.record_type == "int":
            self.__get_next_record = self.__get_next_int
        elif self.record_type == "string":
            self.__get_next_record = self.__get_next_string
        else:
            message = "Unrecognized record type '{}'"
            logger.error(message.format(self.record_type))
            sys.exit()

    # Returns the next int
    def __get_next_int(self):
        self.record += 1
        return self.record

    # Returns the next int
    def __get_next_keyed_int(self):
        self.record += 1
        return (self.key, self.record)

    # Returns the next (random) string
    def __get_next_string(self):
        return "".join(random.choice(
                string.ascii_letters + string.digits) for _ in range(
                                                            self.record_size))

    # Returns the next record (either int or string depending on record_type)
    def get_next(self):
        # TODO: Add the source ID here.
        if self.total_count == self.total_elements:
            return None  # Exhausted
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
        # Do a first round without measuring latency just to warm up
        if self.warm_up and self.total_count <= 100000:
            if self.total_count == 100000:
                logger.info("Finished warmup.")
            return (-1,record)
        self.count += 1
        if self.count == self.period:
            self.count = 0
            # Assign the record generation timestamp
            return (time.time(),record)
        else:
            return(-1,record)

    # Drains the generator and returns the total number of records produced
    def drain(self):
        records = 0
        while self.get_next() is not None:
            records += 1
        return records
