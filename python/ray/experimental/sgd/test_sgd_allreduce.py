#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import time
import json
import subprocess

import ray
from ray.experimental.sgd.tfbench.test_model import TFBenchModel
from ray.experimental.sgd.sgd import DistributedSGD
from ray.tests.cluster_utils import Cluster

parser = argparse.ArgumentParser()
parser.add_argument("--redis-address", default=None, type=str)
parser.add_argument("--num-iters", default=10, type=int)
parser.add_argument("--batch-size", default=1, type=int)
parser.add_argument("--num-workers", default=2, type=int)
parser.add_argument("--grad-shard-bytes", default=10000000, type=int)
parser.add_argument("--devices-per-worker", default=2, type=int)
parser.add_argument("--stats-interval", default=10, type=int)
parser.add_argument("--all-reduce-alg", default="simple", type=str)
parser.add_argument("--object-store-memory", default=None, type=int)
parser.add_argument(
    "--warmup", action="store_true", help="Warm up object store before start.")
parser.add_argument(
    "--strategy", default="allreduce", type=str, help="One of 'simple' or 'ps'")
parser.add_argument(
    "--gpu", action="store_true", help="Use GPUs for optimization")
parser.add_argument(
    '--dump',
    default=None,
    type=str,
    help='A filename to dump the task timeline')
parser.add_argument("--checkpoint", action='store_true')
parser.add_argument("--test-failure", action='store_true')
parser.add_argument("--fail-at", default=None, type=int)

if __name__ == "__main__":
    args, _ = parser.parse_known_args()

    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 20,
        "object_manager_repeated_push_delay_ms": 1000,
        "object_manager_pull_timeout_ms": 1000,
        "gcs_delay_ms": -1,
        "lineage_stash_max_failures": 1,
    })
    plasma_store_memory_gb = 5
    # Start the Ray processes.
    redis_address = args.redis_address
    test_local = redis_address is None
    cluster = None
    node_kwargs = None
    if test_local:
        node_kwargs = {
            "num_cpus": 6,
            "object_store_memory": 10**9,
            "_internal_config": internal_config,
        }
        cluster = Cluster(initialize_head=True, head_node_args=node_kwargs)
        for i in range(args.num_workers):
            node_kwargs["resources"] = {"Node{}".format(i): 100}
            cluster.add_node(**node_kwargs)
        redis_address = cluster.redis_address

    ray.init(
        redis_address=redis_address,
        log_to_driver=True)
    time.sleep(5)

    node_resources = []
    if test_local:
        for worker_index in range(args.num_workers):
            node_resources.append('Node{}'.format(worker_index))
    else:
        nodes = ray.global_state.client_table()
        for node in nodes:
            for resource in node['Resources']:
                if 'Node' in resource:
                    node_resources.append(resource)
        node_resources = node_resources[:args.num_workers]
    assert len(node_resources) == args.num_workers

    model_creator = (
        lambda worker_idx, device_idx: TFBenchModel(
            batch=args.batch_size, use_cpus=not args.gpu))

    checkpoint_interval = -1
    if args.checkpoint:
        checkpoint_interval = args.stats_interval
    sgd = DistributedSGD(
        model_creator,
        num_workers=args.num_workers,
        devices_per_worker=args.devices_per_worker,
        gpu=args.gpu,
        strategy=args.strategy,
        grad_shard_bytes=args.grad_shard_bytes,
        all_reduce_alg=args.all_reduce_alg,
        node_resources=node_resources,
        checkpoint_interval=checkpoint_interval)

    if args.warmup:
        sgd.warmup()

    t = []

    def kill_node():
        if test_local:
            print(node_kwargs)
            node = cluster.list_all_nodes()[-1]
            print("killing", node)
            cluster.remove_node(node)
            cluster.add_node(**node_kwargs)
        else:
            # Pick a node that is not the head node to kill.
            nodes = ray.global_state.client_table()
            head_ip, _ = redis_address.split(':')
            worker_ip = head_ip
            while worker_ip == head_ip:
                node_resource = node_resources.pop(-1)
                nodes = [node for node in nodes if node_resource in node['Resources']]
                assert len(nodes) == 1
                node = nodes[0]
                worker_ip = node['NodeManagerAddress']
            command = [
                    "/home/ubuntu/ray/benchmarks/cluster-scripts/kill_worker.sh",
                    head_ip,
                    worker_ip,
                    #str(args.gcs_delay_ms),
                    str(-1),
                    node_resource,
                    ]
            subprocess.Popen(command)

    num_failed = 0
    for i in range(args.num_iters):
        start = time.time()
        fetch_stats = i % args.stats_interval == 0
        fail_iteration = False
        if args.fail_at is not None:
            fail_iteration = i == args.fail_at
        else:
            if (test_local and i == args.num_iters // 2 and args.test_failure):
                fail_iteration = True
            elif (not test_local and i == args.num_iters // 4 and args.test_failure):
                fail_iteration = True
        print("== Step {} ==".format(i))
        stats = sgd.step(
                fetch_stats=fetch_stats,
                test_failure=fail_iteration,
                kill_node_fn=kill_node,
                num_failed=num_failed)
        num_failed = stats.pop("num_failed")
        ips = ((args.batch_size * args.num_workers * args.devices_per_worker) /
               (time.time() - start))
        print("Iteration time", time.time() - start, "Images per second", ips)
        t.append(ips)
        if fetch_stats:
            print("Current loss", stats)

    print("Peak throughput", max(sum(t[i:i + 5]) / 5 for i in range(len(t))))

    if args.dump is not None:
        events = ray.global_state.chrome_tracing_dump()
        events += ray.global_state.chrome_tracing_object_transfer_dump()
        with open(args.dump, "w") as outfile:
            json.dump(events, outfile)
