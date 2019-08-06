from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import datetime
import os
import time
import signal
import subprocess
import json
import sys
import string

import numpy as np
import ray
from ray.tests.cluster_utils import Cluster
import ray.cloudpickle as pickle
from ray.experimental.internal_kv import _internal_kv_get, _internal_kv_put

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DEBUG = False
WAIT_MS = 10
PROGRESS_LOG_FREQUENCe = 500
CHECKPOINT_DIR = '/tmp/ray-checkpoints'


def debug(*args):
    if DEBUG:
        print(ray.worker.global_worker.current_task_id, *args, flush=True)


class RingWorker(object):
    def __init__(self, worker_index, num_workers, token, task_duration):
        self.worker_index = worker_index
        self.num_workers = num_workers
        self.token = token
        self.task_duration = task_duration
        self.latencies = []
        self.receiver = None
        self.iteration = 0
        self.tasks = 0
        self.num_own_tokens_recv = 0
        _internal_kv_put(self.token, self.iteration, overwrite=True)

    def ip(self):
        return ray.services.get_node_ip_address()

    def add_remote_worker(self, worker):
        self.receiver = worker

    def num_tasks(self):
        return self.tasks

    @ray.method(num_return_vals=0)
    def send(self, token, num_roundtrips_remaining, timestamp):
        self.tasks += 1
        #debug("send", token, num_roundtrips_remaining, timestamp)
        if self.task_duration > 0:
            time.sleep(self.task_duration)

        if token == self.token:
            num_roundtrips_remaining -= 1
            now = time.time()
            self.latencies.append(now - timestamp)
            timestamp = now

        if num_roundtrips_remaining > 0:
            # Forward the token to the next actor in the ring.
            self.receiver.send.remote(token, num_roundtrips_remaining, timestamp)
        elif token == self.token:
            # We received our token back for the last task in this round.
            self.iteration += 1
            _internal_kv_put(self.token, self.iteration, overwrite=True)
            debug("DONE", self.token, self.iteration)

    def get_latencies(self):
        # The first task doesn't count because the timestamp was assigned by the driver.
        latencies = self.latencies[1:][:]
        self.latencies.clear()
        return latencies


def step(iteration, workers, tokens, num_roundtrips, task_duration,
        measure_latency=True):
    tasks = []
    for worker, token in zip(workers, tokens):
        tasks.append(worker.send.remote(token, num_roundtrips, time.time()))

    start = time.time()
    next_iteration = iteration + 1
    for token in tokens:
        worker_iteration = int(_internal_kv_get(token))
        #log.debug('For token %s, worker_iter = %s', token, worker_iteration)
        while worker_iteration != next_iteration:
            #log.debug("Waiting %s ms for token %s", WAIT_MS, token)
            time.sleep(WAIT_MS / 1000)
            clients = ray.global_state.client_table()
            failed_clients = [client for client in clients if not client['IsInsertion']]
            assert len(failed_clients) == 0, "Client failed {}".format(failed_clients[0]['NodeManagerAddress'])
            worker_iteration = int(_internal_kv_get(token))

    if iteration % PROGRESS_LOG_FREQUENCY == 0:
        log.debug("Step %d done after %f ms", iteration, (time.time() - start) * 1000)

    if measure_latency:
        start = time.time()
        latencies = ray.get([worker.get_latencies.remote() for worker in workers])
        log.debug("Got latencies for step %d after %f", iteration, time.time() - start)

        latencies = np.array(latencies)
        latencies -= task_duration * (len(workers))
        latencies /= len(workers)
        latencies = np.reshape(latencies, (num_roundtrips - 1) * len(workers))
        log.info("Mean latency step %d: %f", iteration, np.mean(latencies))
        log.info("Max latency step %d: %f", iteration, np.max(latencies))
        log.info("Stddev latency step %d: %f", iteration, np.std(latencies))
    else:
        latencies = []

    return latencies


def main(args):
    if args.debug:
        log.setLevel(logging.DEBUG)

    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 20,
        "object_manager_repeated_push_delay_ms": 1000,
        "object_manager_pull_timeout_ms": 1000,
        "gcs_delay_ms": args.gcs_delay_ms,
        "use_gcs_only": int(args.gcs_only),
        "lineage_stash_max_failures": -1 if args.nondeterminism else 1,
        "log_nondeterminism": int(args.nondeterminism),
    })
    plasma_store_memory_gb = 5
    # Start the Ray processes.
    test_local = args.redis_address is None
    cluster = None
    node_kwargs = None

    if test_local:
        node_kwargs = {
            "num_cpus": 4,
            "object_store_memory": 10**9,
            "_internal_config": internal_config,
        }
        cluster = Cluster(initialize_head=True, head_node_args=node_kwargs)
        for i in range(args.num_workers):
            node_kwargs["resources"] = {"Node{}".format(i): 100}
            cluster.add_node(**node_kwargs)
        redis_address = cluster.redis_address
    else:
        redis_address = args.redis_address

    ray.init(redis_address=redis_address, log_to_driver=True)

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
    assert len(node_resources) == args.num_workers, "Only found {} nodes".format(len(node_resources))

    # Create workers.
    token_str = list(string.ascii_letters)
    tokens = []
    for token1 in token_str:
        for token2 in token_str:
            tokens.append('{}{}'.format(token1, token2))
    tokens = tokens[:args.num_workers]
    log.debug("Tokens generated: %s", tokens)
    assert len(tokens) == args.num_workers, "Need more tokens"

    workers = []
    for worker_index in range(args.num_workers):
        actor_resources = {node_resources[worker_index]: 1}
        cls = ray.remote(resources=actor_resources)(RingWorker)
        token = tokens[worker_index]
        workers.append(cls.remote(worker_index, args.num_workers, token, args.task_duration))

    # Exchange actor handles.
    waits = []
    for i in range(args.num_workers):
        receiver_index = (i + 1) % args.num_workers
        waits.append(workers[i].add_remote_worker.remote(workers[receiver_index]))
        log.debug("added worker %d %d", i, receiver_index)
    log.debug("Waiting for add_remote_worker tasks to finish")
    ray.get(waits)
    time.sleep(1)
    log.debug("add_remote_worker tasks done")

    # Ensure workers are assigned to unique nodes.
    if not test_local:
        node_ips = ray.get([worker.ip.remote() for worker in workers])
        assert len(set(node_ips)) == args.num_workers

    if args.benchmark_thput:
        benchmark_throughput(workers, tokens, args)
    else:
        benchmark_latency(workers, tokens, args)

    if args.dump is not None:
        log.info("Dumping trace...")
        events = ray.global_state.chrome_tracing_dump()
        events += ray.global_state.chrome_tracing_object_transfer_dump()
        with open(args.dump, "w") as outfile:
            json.dump(events, outfile)

    if test_local:
        log.info("Shutting down...")
        time.sleep(5)
        cluster.shutdown()


def benchmark_throughput(workers, tokens, args):
    i = 0

    warmup_duration = args.thput_warmup_duration
    log.info("Warming up for %s s...", warmup_duration)
    warmup_end_ts = time.time() + warmup_duration
    while time.time() < warmup_end_ts:
        step(i, workers, tokens, args.num_roundtrips, args.task_duration, False)
        i += 1
    num_warmup_iters = i
        
    measurement_duration = args.thput_measurement_duration
    log.info("Measuring for %s s...", measurement_duration)
    measurement_end_ts = time.time() + measurement_duration 
    while time.time() < measurement_end_ts:
        step(i, workers, tokens, args.num_roundtrips, args.task_duration, False)
        i += 1

    num_measurement_iters = i - num_warmup_iters
    # TODO figure out why this calculation is wrong
    #tasks_per_worker = num_measurement_iters * args.num_roundtrips * args.num_workers
    #tasks_per_worker_per_sec = tasks_per_worker / args.thput_measurement_duration
    #tasks_sys_per_sec = tasks_per_worker_per_sec * args.num_workers
    num_tasks = sum(ray.get([worker.num_tasks.remote() for worker in workers]))
    sys_throughput = int(num_tasks / measurement_duration)
    avg_worker_throughput = int(sys_throughput / args.num_workers)

    log.info('Worker throughput (avg): %s tasks/s', avg_worker_throughput)
    log.info('System throughput: %s tasks/s', sys_throughput)
    log.info('Steps: %s', num_measurement_iters)


def benchmark_latency(workers, tokens, args):
    if args.record_latency:
        if args.latency_file is None:
            latency_file = "latency-{}-workers-{}.txt".format(
                len(workers),
                str(datetime.datetime.now()))
        else:
            latency_file = args.latency_file
        log.info("Logging latency to file %s", latency_file)

    latencies = []
    for i in range(args.num_iterations):
        log.info("Starting iteration %d", i)
        results = step(i, workers, tokens, args.num_roundtrips, args.task_duration)
        latencies.append(results)
    
    # Write latency
    if latency_file is not None:
        with open(latency_file, 'a+') as f:
            log.info('Writing to ', latency_file)
            for i, latency in enumerate(latencies):
                for l in latency:
                    f.write('{},{}\n'.format(i, l))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
         '--num-workers',
         default=3,
         type=int,
         help='The # of workers to use.')
    parser.add_argument(
        '--num_roundtrips',
        default=100,
        type=int,
        help='The # of roundtrips a token takes per step.')
    parser.add_argument(
        '--task-duration',
        default=0,
        type=float,
        help='Duration of each task in seconds.')
    parser.add_argument(
        '--redis-address',
        default=None,
        type=str,
        help='The address of the redis server.')
    parser.add_argument(
        '--dump',
        default=None,
        type=str,
        help='A filename to dump the task timeline')
    parser.add_argument(
        '--gcs-delay-ms',
        default=-1,
        help='Delay when writing back to GCS. The default is to use the lineage stash.')
    parser.add_argument(
        '--gcs-only',
        action='store_true')
    parser.add_argument(
        '--nondeterminism',
        action='store_true')
    parser.add_argument(
        '--debug',
        action='store_true')
    # Latency benchmark options
    parser.add_argument(
        '--record-latency',
        action='store_true',
        help='Whether to record the latency (latency benchmark only)')
    parser.add_argument(
        '--latency-file',
        default=None,
        help='File to record the latency (latency benchmark only)')
    parser.add_argument(
        '--num-iterations',
        default=10,
        type=int,
        help='The # of iterations (latency benchmark only).')
    # Throughput benchmark options
    parser.add_argument(
        '--benchmark-thput',
        action='store_true',
        help='Benchmark throughput instead of latency (throughput benchmark only)')
    parser.add_argument(
        '--thput-warmup-duration',
        default=10,
        type=int,
        help='Warmup time in seconds (throughput benchmark only)')
    parser.add_argument(
        '--thput-measurement-duration',
        default=50,
        type=int,
        help='Throughput measurement time in seconds (throughput benchmark only)')
    #parser.add_argument(
    #    '--throughput-file',
    #    default=None,
    #    help='File to record the throughput')

    args = parser.parse_args()

    main(args)
