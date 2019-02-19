from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os
import time
import sys

import numpy as np
import ray

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DEBUG = False

def debug(*args):
    if DEBUG:
        print(*args, flush=True)


def compute_batch_indices(total_size, num_batches):
    """
    :param total_size: Total number of items to split into batches.
    :param batch_size: Size of each batch.
    :return: A list of 2-tuples.
             Each 2-tuple is a segment of indices corresponding to items of size batch_size.
             The size of the list is total_size / batch_size.
    """
    batch_size = int(np.floor(total_size / num_batches))
    remainder = total_size % num_batches

    start_index = 0
    batches = []
    for i in range(num_batches):
        # Use round-robin to determine batch sizes.
        end_index = start_index + batch_size
        if remainder > 0:
            remainder -= 1
            end_index += 1
        batches.append((start_index, end_index))
        start_index = end_index
    assert start_index == total_size
    return batches


class WeightPartition(object):

    def __init__(self, buffer_size, num_batches, buffer_data=None):
        self.buffer_size = buffer_size
        self.num_batches = num_batches
        self.batch_intervals = compute_batch_indices(self.buffer_size, self.num_batches)

        if buffer_data is None:
            buffer_data = np.ones(self.buffer_size).astype(np.float32)
        # Cache the batches.
        self.batches = [None] * self.num_batches
        self.set_weights(buffer_data)

    def set_weights(self, buffer_data):
        self.buffer_data = buffer_data
        for i in range(len(self.batch_intervals)):
            s, e = self.batch_intervals[i]
            self.batches[i] = buffer_data[s:e]

    def get_weights(self):
        for i in range(len(self.batch_intervals)):
            s, e = self.batch_intervals[i]
            self.buffer_data[s:e] = self.batches[i]
        return self.buffer_data

    def get_partition(self, i):
        return self.batches[i]

    def set_partition(self, i, batch):
        self.batches[i] = batch

    def add_partition(self, i, batch):
        self.batches[i] += batch


class RingAllReduceWorker(object):

    def __init__(self, worker_index, num_workers, buffer_size):
        self.worker_index = worker_index
        self.num_workers = num_workers
        self.workers = {}
        self.reset(buffer_size, None)

    def ip(self):
        return ray.services.get_node_ip_address()

    def add_remote_worker(self, index, worker):
        self.workers[index] = worker

    def get_weights(self):
        return self.weight_partition.get_weights()

    def reset(self, buffer_size, weights):
        self.weight_partition = WeightPartition(buffer_size, self.num_workers, weights)
        self.out_oid = None
        self.done_oid = None
        self.aggregate_received = []
        self.broadcast_received = []
        self.done = False

        self.execute_received = False
        self.receives = []

    def execute(self, out_oid, done_oid):
        debug("EXECUTE: worker", self.worker_index)
        assert not self.execute_received

        self.execute_received = True
        self.out_oid = out_oid
        self.done_oid = done_oid
        self.send(self.worker_index, True)
        # Resend any buffered data that was received before the allreduce
        # started.
        while self.receives:
            index, aggregate, batch_buffer = self.receives.pop(0)
            self.receive(index, aggregate, batch_buffer)

    def send(self, index, aggregate):
        assert not self.done
        debug("SEND: worker", self.worker_index, "batch", index, aggregate)
        batch_buffer = self.weight_partition.get_partition(index)
        receiver = self.workers[(self.worker_index + 1) % self.num_workers]
        receiver.receive.remote(index, aggregate, batch_buffer)

    def receive(self, index, aggregate, batch_buffer):
        assert not self.done
        debug("RECEIVE: worker", self.worker_index, "batch", index, aggregate)
        if not self.execute_received:
            # If we haven't received the allreduce start message yet, buffer
            # the received data. It will be resent once we get the first
            # `execute` task.
            self.receives.append((index, aggregate, batch_buffer))
            return

        # Process the received data.
        if aggregate:
            self.weight_partition.add_partition(index, batch_buffer)
            received = self.aggregate_received
        else:
            self.weight_partition.set_partition(index, batch_buffer)
            received = self.broadcast_received

        #print(self.worker_index, index, self.aggregate_received, self.broadcast_received, aggregate, flush=True)
        if DEBUG:
            assert index not in received
        received.append(index)

        if aggregate:
            # If this is the last chunk to be sent by our sender, then this
            # chunk has been fully reduced. Send it to the next worker and
            # signal it to just apply the value instead of aggregating.
            if index == (self.worker_index + 1) % self.num_workers:
                aggregate = False
            # Forward the chunk to the next worker.
            self.send(index, aggregate)
        elif index != (self.worker_index + 2) % self.num_workers:
            # This chunk has been fully reduced. Only forward the chunk to the
            # next worker if they haven't already seen it.
            self.send(index, aggregate)

        if len(self.aggregate_received) + len(self.broadcast_received) + 2 == self.num_workers * 2:
            self.finish()

    def finish(self):
        debug("FINISH: worker", self.worker_index)
        # Put the final values.
        out_oid = ray.ObjectID(self.out_oid)
        ray.worker.global_worker.put_object(out_oid, self.weight_partition.get_weights())
        done_oid = ray.ObjectID(self.done_oid)
        ray.worker.global_worker.put_object(done_oid, True)

        self.done = True


def main(redis_address, num_workers, data_size, num_iterations, debug):
    ray.init(redis_address=redis_address)

    # Create workers.
    workers = []
    for worker_index in range(num_workers):
        if redis_address is None:
            cls = ray.remote(RingAllReduceWorker)
        else:
            cls = ray.remote(resources={'Actor' + str(worker_index+1): 1})(RingAllReduceWorker)
        workers.append(cls.remote(worker_index, num_workers, data_size))

    # Exchange actor handles.
    for i in range(num_workers):
        for j in range(num_workers):
            workers[i].add_remote_worker.remote(j, workers[j])

    # Ensure workers are assigned to unique nodes.
    if redis_address is not None:
        node_ips = ray.get([worker.node_address.remote() for worker in workers])
        assert(len(set(node_ips)) == args.num_workers)

    for i in range(num_iterations):
        log.info("Starting iteration %d", i)

        # Get the initial weights on each of the workers so we can check the
        # results.
        weights = []
        if debug:
            weights = ray.get([worker.get_weights.remote() for worker in workers])

        # Start the send on each worker.
        start = time.time()
        output_oids = []
        done_oids = []
        for worker in workers:
            output_oid = np.random.bytes(20)
            done_oid = np.random.bytes(20)
            output_oids.append(output_oid)
            done_oids.append(done_oid)
            worker.execute.remote(output_oid, done_oid)

        done_oids = [ray.ObjectID(done_oid) for done_oid in done_oids]
        ray.get(done_oids)
        log.info("Finished iteration %d in %f", i, time.time() - start)

        # Check the results on each of the workers.
        if debug:
            expected = sum(weights)
            all_reduced = ray.get([ray.ObjectID(output_oid) for output_oid in output_oids])
            for reduced in all_reduced:
                assert np.allclose(expected, reduced)
        ray.get([worker.reset.remote(data_size, None) for worker in workers])

    if DEBUG:
        ray.global_state.chrome_tracing_dump(filename="allreduce.json")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument('--check-results', action='store_true', help='Whether to check results.')
    parser.add_argument('--num-workers', default=3, type=int, help='The number of workers to use.')
    parser.add_argument('--size', default=25000000, type=int,
                        help='The number of 32bit floats to use.')
    parser.add_argument('--num-iterations', default=10, type=int,
                        help='The number of iterations.')
    parser.add_argument('--redis-address', default=None, type=str,
                        help='The address of the redis server.')
    args = parser.parse_args()

    main(args.redis_address, args.num_workers, args.size, args.num_iterations, args.check_results)
