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
logger.setLevel("DEBUG")

CLUSTER_NODE_PREFIX = "Node_"

# Uses Linux's taskset command to pin each Python process to a CPU core
# Make sure that all python processes are up and running before calling this
def pin_processes():
    # Pins each python process to a specific core
    cmd_pids = ["pgrep", "python"]
    result = subprocess.check_output(cmd_pids)
    pids = [pid for pid in str(result.decode("ascii").strip()).split("\n")]
    print("Found {} python processes with PIDs: {}".format(len(pids), pids))
    cmd_pin = ["taskset", "-pc", None, None]
    for i, pid in enumerate(pids):
        cmd_pin[2] = str(hex(i))  # Affinity mask
        cmd_pin[3] = pid
        subprocess.call(cmd_pin)

# Returns the node ids in a Ray cluster
def get_cluster_node_ids():
    node_ids = []
    for node in ray.global_state.client_table():
        for node_id in node["Resources"].keys():
            if "CPU" not in node_id and "GPU" not in node_id:
                node_ids.append(id)
    return node_ids

# Starts Ray with the given configuration.
# Assumes a chain dataflow where all stages have the same level of parallelism
# except sources, which can be configured arbitrarily to meet rate targets.
# Actor placement is done based on a N:1 mapping from dataflow stages to
# cluster nodes, i.e. a node might host more than one stages, but all operator
# instances of a particular stage will run at the same node
def start_ray(num_nodes, num_redis_shards, plasma_memory,
              redis_max_memory, num_stages, dataflow_parallelism,
              num_sources, pin_processes):
    # Simulate a cluster on a single machine
    cluster = Cluster()
    # 'num_stages' is the user-defined parameter that does not include sources
    # and sinks. We also need to count the actor for tracking progress
    num_actors = num_sources + dataflow_parallelism * (num_stages + 1) + 1
    logger.info("Total number of required actors: {}".format(num_actors))
    num_cpus = multiprocessing.cpu_count()
    if num_cpus < num_actors:
        part_1 = "Dataflow contains {} actors".format(num_actors)
        part_2 = "but only {} available CPUs were found.".format(num_cpus)
        logger.error(part_1 + " " + part_2)
        sys.exit()
    # The 'actors_per_stage' list includes only source and map stages
    actors_per_stage = [num_sources]
    actors_per_stage.extend([dataflow_parallelism for _ in range(num_stages)])
    stages_per_node = math.trunc(math.ceil(len(actors_per_stage) / num_nodes))
    logger.info("Number of stages per node: {}".format(stages_per_node))
    assigned_actors = 0
    node_actors = 1  # The monitoring actor runs at the first cluster node
    for i in range(num_nodes):
        remaining_actors = num_actors - assigned_actors
        if remaining_actors == 0:  # No more nodes are needed
            break
        low = i * stages_per_node
        high = (i + 1) * stages_per_node
        if high >= len(actors_per_stage):
            node_actors += dataflow_parallelism  # Sinks run at the last node
            high = len(actors_per_stage)
        node_actors += sum(n for n in actors_per_stage[low:high])
        # Add cluster node
        cluster.add_node(
            num_redis_shards=num_redis_shards if i == 0 else None,
            num_cpus=node_actors,
            num_gpus=0,
            resources={CLUSTER_NODE_PREFIX+str(i): node_actors},
            object_store_memory=plasma_memory,
            redis_max_memory=redis_max_memory)
        assigned_actors += node_actors
        logger.info("Added node {} with {} CPUs".format(i, node_actors))
        node_actors = 0
    # Start ray
    ray.init(redis_address=cluster.redis_address)

    if pin_processes:  # Pin python processes to CPU cores
        logger.info("Waiting to pin python processes to cores...")
        time.sleep(5)  # Wait a bit for Ray to start
        pin_processes()

def shutdown_ray(sleep=0):
    ray.shutdown()
    time.sleep(sleep)
