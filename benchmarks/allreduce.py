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

import numpy as np
import ray
from ray.tests.cluster_utils import Cluster
import ray.cloudpickle as pickle

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DEBUG = False
CHECKPOINT_DIR = '/tmp/ray-checkpoints'


def debug(*args):
    if DEBUG:
        print(
            "task ID:",
            ray.worker.global_worker.current_task_id,
            *args,
            flush=True)


def compute_batch_indices(shape, num_batches):
    """
    :param total_size: Total number of items to split into batches.
    :param batch_size: Size of each batch.
    :return: A list of 2-tuples.
             Each 2-tuple is a segment of indices corresponding to items of
             size batch_size. The size of the list is total_size / batch_size.
    """
    if type(shape) != int:
        total_size = int(shape[0])
    else:
        total_size = shape

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
        self.batch_intervals = compute_batch_indices(self.buffer_size,
                                                     self.num_batches)

        if buffer_data is None:
            buffer_data = np.ones(self.buffer_size).astype(np.float32)
        # Cache the batches.
        self.batches = [None] * self.num_batches
        # If self.committed[i] is True, then the contents of self.batches match
        # those of self.buffer_data for batch i.
        self.committed = [False] * self.num_batches
        self.set_weights(buffer_data)
        self.buffer_data = buffer_data
        self.committed = [True] * self.num_batches

    def set_weights(self, buffer_data):
        for i in range(len(self.batch_intervals)):
            s, e = self.batch_intervals[i]
            self.batches[i] = buffer_data[s:e]
            self.committed[i] = False

    def get_weights(self):
        assert all(self.committed)
        return self.buffer_data

    def get_partition(self, i):
        return self.batches[i]

    def set_partition(self, i, batch):
        self.batches[i] = batch
        self.committed[i] = False

    def commit_partition(self, i):
        partition = self.get_partition(i)
        s, e = self.batch_intervals[i]
        self.buffer_data[s:e] = partition
        self.committed[i] = True

    def add_partition(self, i, batch, in_place):
        if in_place:
            self.batches[i] += batch
        else:
            self.batches[i] = self.batches[i] + batch


class RingAllReduceWorker(object):
    def __init__(self, worker_index, num_workers, buffer_size, in_place):
        self.worker_index = worker_index
        self.num_workers = num_workers
        self.workers = {}
        self.reset(buffer_size=buffer_size)
        self.num_iterations = 0
        self.in_place = in_place
        self.log = []
        self.cache = []

    def ip(self):
        return ray.services.get_node_ip_address()

    def get_pid(self):
        return os.getpid()

    def add_remote_worker(self, index, worker):
        self.workers[index] = worker

    def get_weights(self):
        debug("GET_WEIGHTS", self.num_iterations, ": worker",
              self.worker_index)
        return self.weight_partition.get_weights()

    def reset(self, buffer_size=None, weights=None):
        if buffer_size is not None:
            self.weight_partition = WeightPartition(buffer_size,
                                                    self.num_workers, weights)
        self.done_oid = None
        self.final_oid = None
        self.out_oids = [None] * self.num_workers
        self.aggregate_received = []
        self.broadcast_received = []

        self.execute_received = False
        self.receives = []

    def get_receiver(self):
        return self.workers[(self.worker_index + 1) % self.num_workers]

    def execute(self, input_data, done_oid, final_oid):
        """
        If final_oid is set, then the concatenated final output will be written
        to this object ID before the allreduce is considered to be done.

        If done_oid is set, then the object IDs of the reduced chunks will be
        written to this object ID once the allreduce is considered to be done.
        These object IDs can be retrieved and concatenated to produce the final
        output.
        """
        debug("EXECUTE", self.num_iterations, ": worker", self.worker_index, ray.ObjectID(final_oid))
        assert not self.execute_received

        # Update our state.
        self.execute_received = True
        self.done_oid = done_oid
        self.final_oid = final_oid

        self.weight_partition.set_weights(input_data)
        # Send the first chunk to our receiver.
        self.send(self.worker_index, True)

        if self.in_place:
            with ray.profiling.profile("init_weights"):
                input_data = np.copy(input_data)
                input_data.flags.writeable = True
                self.weight_partition.set_weights(input_data)

        # Resend any buffered data that was received before the allreduce
        # started.
        while self.receives:
            index, aggregate, batch_buffer, batch_id = self.receives.pop(0)
            self.receive(index, aggregate, batch_buffer, batch_id)

        # We no longer need the input, so delete it.
        input_id = ray.worker.global_worker.get_argument_id(input_data)
        input_id = ray.pyarrow.plasma.ObjectID(input_id.binary())
        ray.worker.global_worker.plasma_client.delete([input_id])

    def send(self, index, aggregate, batch_id=None):
        debug("SEND", self.num_iterations, ": worker", self.worker_index,
              "batch", index, aggregate)
        batch_buffer = self.weight_partition.get_partition(index)
        receiver = self.get_receiver()
        # Check if the data was received by someone else. Then, we can forward
        # it.
        if batch_id is None:
            # The data was not received by someone else, so we cannot forward
            # it. Put the object in the local object store first.
            batch_id = ray.put(batch_buffer)
        receiver.receive.remote(index, aggregate, batch_id)
        return batch_id

    def receive(self, index, aggregate, batch_buffer, batch_id=None):
        if batch_id is None:
            batch_id = ray.worker.global_worker.get_argument_id(batch_buffer)
        debug("RECEIVE", self.num_iterations, ": worker", self.worker_index,
              "batch", index, aggregate)
        if not self.execute_received:
            # If we haven't received the allreduce start message yet, buffer
            # the received data. It will be resent once we get the first
            # `execute` task.
            self.receives.append((index, aggregate, batch_buffer, batch_id))
            return

        # Process the received data.
        if aggregate:
            # We received a partially reduced chunk. Add the partition.
            with ray.profiling.profile("add_partition"):
                self.weight_partition.add_partition(index, batch_buffer, self.in_place)
            received = self.aggregate_received
            # If this is the last chunk to be sent by our sender, then this
            # chunk has been fully reduced. Send it to the next worker, but
            # signal it to just overwrite its value instead of aggregating.
            if index == (self.worker_index + 1) % self.num_workers:
                aggregate = False
            # Forward the chunk to the next worker. Get the object ID where the
            # sent data was stored since we need to remember it if the chunk
            # was fully reduced.
            batch_id = self.send(index, aggregate)
        else:
            # We received a fully reduced chunk. Overwrite our partition.
            with ray.profiling.profile("set_partition"):
                self.weight_partition.set_partition(index, batch_buffer)
            received = self.broadcast_received
            # Only forward the chunk to the next worker if they haven't already
            # seen it.
            if index != (self.worker_index + 2) % self.num_workers:
                self.send(index, aggregate, batch_id=batch_id)

        if DEBUG:
            debug(self.worker_index, index, self.aggregate_received,
                  self.broadcast_received, aggregate)
            assert index not in received
        received.append(index)

        if not aggregate:
            # The sent or received chunk was fully reduced, so remember it.
            self.out_oids[index] = batch_id
            self.weight_partition.commit_partition(index)
        else:
            if batch_id is not None:
                # Try to evict old messages to leave space for the allreduce output.
                self.cache.append(ray.pyarrow.plasma.ObjectID(batch_id.binary()))
                if len(self.cache) > self.num_workers:
                    ray.worker.global_worker.plasma_client.delete([self.cache.pop(0)])

        # We've received all of the reduced chunks. Finish the allreduce.
        if len(self.aggregate_received) + len(
                self.broadcast_received) + 2 == self.num_workers * 2:
            assert all(out_oid is not None for out_oid in self.out_oids)

            #  Necessary for tensorflow?
            # self_handle = self.workers[self.worker_index]
            # self_handle.finish.remote(*self.out_oids)

            self.finish(self.out_oids, self.final_oid, self.done_oid)
            if self.checkpoint_interval > 0:
                self.log.append((self.out_oids, self.final_oid, self.done_oid))
            else:
                self.log = [(self.out_oids, self.final_oid, self.done_oid)]


    def finish(self, out_oids, final_oid, done_oid):
        debug("FINISH", self.num_iterations, ": worker", self.worker_index, ray.ObjectID(final_oid))
        # Store the concatenated data in the final output ObjectID, if one was
        # provided.
        if final_oid is not None:
            with ray.profiling.profile("concatenate_out"):
                final_oid = ray.ObjectID(final_oid)
                final_output = self.weight_partition.get_weights() / self.num_workers
                with ray.profiling.profile("store_out"):
                    ray.worker.global_worker.put_object(
                        final_oid, final_output)

        # Store pointers to the shards to notify any callers that we've
        # received.
        if done_oid is not None:
            with ray.profiling.profile("store_done"):
                done_oid = ray.ObjectID(done_oid)
                # Add this task's output so that callers can schedule tasks
                # after this task.
                ray.worker.global_worker.put_object(done_oid, out_oids)


class CheckpointableRingAllReduceWorker(RingAllReduceWorker,
                                        ray.actor.Checkpointable):
    def __init__(self, worker_index, num_workers, buffer_size, in_place,
            checkpoint_dir, checkpoint_interval):
        super(CheckpointableRingAllReduceWorker, self).__init__(
            worker_index, num_workers, buffer_size, in_place)

        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_attrs = [
            "checkpoint_dir",
            "worker_index",
            "num_workers",
            "workers",
            "final_oid",
            "log",
            "done_oid",
            "num_iterations",
            "in_place",
        ]
        self._should_checkpoint = False

        # Create the checkpoint directory.
        try:
            os.makedirs(self.checkpoint_dir)
        except FileExistsError:
            pass


    def finish(self, *outputs):
        super(CheckpointableRingAllReduceWorker, self).finish(*outputs)
        self._should_checkpoint = True

    def should_checkpoint(self, checkpoint_context):
        should_checkpoint = self._should_checkpoint
        self._should_checkpoint = False
        return should_checkpoint

    def save_checkpoint(self, actor_id, checkpoint_id):
        with ray.profiling.profile("save_checkpoint"):
            debug("Saving checkpoint", self.num_iterations, checkpoint_id)

            start = time.time()
            checkpoint = {}
            for attr in self.checkpoint_attrs:
                checkpoint[attr] = getattr(self, attr)
            checkpoint["checkpoint_id"] = checkpoint_id
            checkpoint["put_index"] = ray.worker.global_worker.task_context.put_index
            size = sys.getsizeof(checkpoint)
            checkpoint = pickle.dumps(checkpoint)
            end = time.time()
            debug("pickle took", end - start, "checkpoint bytes", size, "checkpoint size", len(checkpoint))

            start = time.time()
            checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_id.hex())
            with open(checkpoint_path, 'wb+') as f:
                f.write(checkpoint)
            end = time.time()
            debug("write took", end - start)

        with ray.profiling.profile("save_physical_checkpoint"):
            debug("Length of log is", len(self.log), self.checkpoint_interval, len(self.log) == self.checkpoint_interval)
            if len(self.log) == self.checkpoint_interval:
                #checkpoint = self.weight_partition.get_weights()
                #checkpoint_path = os.path.join(self.checkpoint_dir, "{}-{}.npy".format(actor_id.hex(), self.num_iterations))
                #np.save(checkpoint_path, checkpoint)
                self.log.clear()

        # Reset our state once the checkpoint completes.
        self.reset()
        self.num_iterations += 1

    def restore(self, checkpoint_id):
        debug("Trying to restore", checkpoint_id)
        checkpoint_path = os.path.join(self.checkpoint_dir,
                                       checkpoint_id.hex())
        with open(checkpoint_path, 'rb') as f:
            checkpoint = pickle.loads(f.read())

        # Check whether all of the output ObjectIDs are available. If not, then
        # we cannot restore from this checkpoint.
        out_oids, final_oid, done_oid = checkpoint["log"][-1]
        _, lost = ray.wait(out_oids, num_returns=len(out_oids), timeout=0,
                           request_once=False)
        if lost:
            return False

        ray.worker.global_worker.task_context.put_index = checkpoint.pop("put_index")
        for attr in self.checkpoint_attrs:
            setattr(self, attr, checkpoint[attr])
        for handle in self.workers.values():
            handle.reset_handle_id()

        for out_oids, final_oid, done_oid in self.log:
            with ray.profiling.profile("restore_log"):
                debug("RESTORE", out_oids, final_oid)
                outputs = ray.get(out_oids)
                # Restore the all-reduced data.
                for i, output in enumerate(outputs):
                    self.weight_partition.set_partition(i, output)
                    self.weight_partition.commit_partition(i)
                # Restore the final object IDs indicating that this all-reduce has
                # finished.
                self.finish(out_oids, final_oid, done_oid)
        self.reset()
        self.num_iterations += 1
        self._should_checkpoint = False

        debug("Restored", checkpoint_id)
        return True

    def load_checkpoint(self, actor_id, available_checkpoints):
        while available_checkpoints:
            checkpoint_id = available_checkpoints.pop(0).checkpoint_id
            if self.restore(checkpoint_id):
                return checkpoint_id
        # We were not able to restore from any of the available checkpoints.
        return None

    def checkpoint_expired(self, actor_id, checkpoint_id):
        pass

    def heartbeat(self):
        receiver = self.get_receiver()
        # The timeout here could be 0, but the ray.wait bug needs to be fixed
        # first.
        ray.wait([receiver._ray_actor_cursor], num_returns=1, timeout=0)
        # NOTE: We cannot submit any tasks here, because they can potentially
        # end up with the same task ID as an actual task from the previous run
        # (e.g., a receive task). Instead, we just call `ray.wait` on the last
        # task that we submitted.


def allreduce(workers, test_failure, check_results, kill_node_fn, num_failed, checkpoint_interval):
    # Get the initial weights on each of the workers so we can check the
    # results.
    weight_ids = [
        worker.get_weights.remote() for i, worker in enumerate(workers)
    ]
    if check_results:
        weights = []
        weights = ray.get(weight_ids)

    # Start the send on each worker.
    start = time.time()
    done_oids = []
    out_oids = []
    executed = []
    for i, worker in enumerate(workers):
        done_oid = np.random.bytes(20)
        done_oids.append(done_oid)
        if checkpoint_interval > 0:
            out_oid = None
        else:
            out_oid = np.random.bytes(20)
        out_oids.append(out_oid)
        executed.append(
            worker.execute.remote(weight_ids[i], done_oid, out_oid))

    # If we are testing locally with failures on, kill a worker halfway
    # through.
    if test_failure:
        time_to_sleep = np.random.rand() * 0.3
        time.sleep(time_to_sleep)
        kill_node_fn()

    timeout_s = 0.1
    done_oids = [ray.ObjectID(done_oid) for done_oid in done_oids]

    # Suppress reconstruction since these object IDs were generated
    # out-of-band.
    while True:
        try:
            all_output_oids = ray.get(
                done_oids, suppress_reconstruction=True, timeout=timeout_s)
            break
        except ray.exceptions.RayGetTimeoutError:
            clients = ray.global_state.client_table()
            failed = len([client for client in clients if not client['IsInsertion']]) > 0
            if failed > num_failed:
                num_failed = failed
                heartbeats = [worker.heartbeat.remote() for worker in workers]
                ray.get(heartbeats)

    latency = time.time() - start
    log.info("Finished in %f", latency)
    # Check the results on each of the workers.
    if check_results:
        # Check that all of the workers end up with the same shards.
        assert all([
            output_oids == all_output_oids[0]
            for output_oids in all_output_oids
        ])

        # Check that the shards contain the correct values.
        expected = sum(weights) / len(workers)
        outputs = ray.get([ray.ObjectID(out_oid) for out_oid in out_oids])
        for output in outputs:
            assert np.allclose(expected, output)

    return latency, num_failed


def main(redis_address, test_single_node, num_workers, data_size,
         num_iterations, check_results, dump, test_failure, record_latency,
         gcs_delay_ms, latency_file, checkpoint_interval):
    if record_latency and latency_file is None:
        latency_file = "latency-{}-mb-{}-workers-{}.txt".format(
                data_size * 4 // 1e6,
                num_workers,
                str(datetime.datetime.now()))
        log.info("Logging latency to file %s", latency_file)

    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 20,
        "object_manager_repeated_push_delay_ms": 1000,
        "object_manager_pull_timeout_ms": 1000,
        "gcs_delay_ms": gcs_delay_ms,
        "lineage_stash_max_failures": 1,
    })
    plasma_store_memory_gb = 5
    # Start the Ray processes.
    test_local = redis_address is None
    cluster = None
    node_kwargs = None
    if test_single_node:
        resources = {"Node{}".format(i): 1 for i in range(num_workers)}
        ray.init(
            resources=resources,
            object_store_memory=plasma_store_memory_gb * 10**9 * num_workers,
            _internal_config=internal_config)
    else:
        if test_local:
            node_kwargs = {
                "num_cpus": 1,
                "object_store_memory": 10**9,
                "_internal_config": internal_config,
            }
            cluster = Cluster(initialize_head=True, head_node_args=node_kwargs)
            for i in range(num_workers):
                node_kwargs["resources"] = {"Node{}".format(i): 100}
                cluster.add_node(**node_kwargs)
            redis_address = cluster.redis_address

        ray.init(redis_address=redis_address, log_to_driver=True)

    node_resources = []
    if test_local:
        for worker_index in range(num_workers):
            node_resources.append('Node{}'.format(worker_index))
    else:
        nodes = ray.global_state.client_table()
        for node in nodes:
            for resource in node['Resources']:
                if 'Node' in resource:
                    node_resources.append(resource)
        node_resources = node_resources[:num_workers]
    assert len(node_resources) == num_workers

    # Create the checkpoint directory.
    checkpoint_dir = os.path.join(
        CHECKPOINT_DIR, ray.worker.global_worker.task_driver_id.hex())
    try:
        os.mkdir(CHECKPOINT_DIR)
    except FileExistsError:
        pass
    os.mkdir(checkpoint_dir)

    # Create workers.
    in_place = False
    workers = []
    for worker_index in range(num_workers):
        actor_resources = {node_resources[worker_index]: 1}
        cls = ray.remote(
            resources=actor_resources,
            max_reconstructions=100)(CheckpointableRingAllReduceWorker)
        workers.append(
            cls.remote(worker_index, num_workers, data_size, in_place,
                       checkpoint_dir, checkpoint_interval))

    # Exchange actor handles.
    for i in range(num_workers):
        receiver_index = (i + 1) % num_workers
        workers[i].add_remote_worker.remote(receiver_index, workers[receiver_index])

    # Ensure workers are assigned to unique nodes.
    if not test_local:
        node_ips = ray.get(
            [worker.ip.remote() for worker in workers])
        assert (len(set(node_ips)) == num_workers)

    def kill_node():
        if test_local:
            if test_single_node:
                worker = workers[-1]
                pid = ray.get(worker.get_pid.remote())
                os.kill(pid, signal.SIGKILL)
            else:
                print(node_kwargs)
                node = cluster.list_all_nodes()[-1]
                print("killing", node)
                cluster.remove_node(node)
                cluster.add_node(**node_kwargs)
        else:
            nodes = ray.global_state.client_table()
            node_resource = node_resources[-1]
            nodes = [node for node in nodes if node_resource in node['Resources']]
            assert len(nodes) == 1
            node = nodes[0]
            worker_ip = node['NodeManagerAddress']
            head_ip, _ = redis_address.split(':')
            command = [
                    "/home/ubuntu/ray/benchmarks/cluster-scripts/kill_worker.sh",
                    head_ip,
                    worker_ip,
                    str(args.gcs_delay_ms),
                    node_resource,
                    ]
            subprocess.Popen(command)

    # Don't check the results if the checkpoint to disk interval is set.
    if checkpoint_interval > 0:
        check_results = False

    num_failed = 0
    latencies = []
    for i in range(num_iterations):
        log.info("Starting iteration %d", i)

        fail_iteration = False
        if (test_local and i == num_iterations // 2 and test_failure):
            fail_iteration = True
        elif (not test_local and i == num_iterations // 4 and test_failure):
            fail_iteration = True
        latency, num_failed = allreduce(workers, fail_iteration, check_results, kill_node, num_failed, checkpoint_interval)
        latencies.append(latency)
        time.sleep(1)

    if latency_file is not None:
        with open(latency_file, 'w+') as f:
            for latency in latencies:
                f.write('{}\n'.format(latency))

    if dump is not None:
        events = ray.global_state.chrome_tracing_dump()
        events += ray.global_state.chrome_tracing_object_transfer_dump()
        with open(dump, "w") as outfile:
            json.dump(events, outfile)

    if test_local and not test_single_node:
        cluster.shutdown()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
        '--check-results',
        action='store_true',
        help='Whether to check results.')
    parser.add_argument(
        '--num-workers',
        default=3,
        type=int,
        help='The number of workers to use.')
    parser.add_argument(
        '--test-single-node',
        action='store_true',
        help='Whether to test on a single raylet')
    parser.add_argument(
        '--size',
        default=25000000,
        type=int,
        help='The number of 32bit floats to use.')
    parser.add_argument(
        '--num-iterations',
        default=10,
        type=int,
        help='The number of iterations.')
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
        '--record-latency',
        action='store_true',
        help='Whether to record the latency')
    parser.add_argument(
        '--latency-file',
        default=None,
        help='File to record the latency')
    parser.add_argument(
        '--test-failure',
        action='store_true',
        help='Whether or not to test worker failure')
    parser.add_argument(
        '--gcs-delay-ms',
        default=-1,
        help='Delay when writing back to GCS. The default is to use the lineage stash.')
    parser.add_argument(
        '--checkpoint-interval',
        default=-1,
        type=int,
        help='Number of iterations before checkpointing to disk')
    args = parser.parse_args()

    main(args.redis_address, args.test_single_node, args.num_workers,
         args.size, args.num_iterations, args.check_results, args.dump,
         args.test_failure, args.record_latency, args.gcs_delay_ms,
         args.latency_file, args.checkpoint_interval)
