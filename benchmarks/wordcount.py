import argparse
import json
import time
import os
from collections import defaultdict
import string
import numpy as np
import hashlib
import logging

import ray
from ray.tests.cluster_utils import Cluster
import ray.cloudpickle as pickle
from ray.experimental import named_actors


DEBUG = True
CHECKPOINT_DIR = '/tmp/ray-checkpoints'


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

def debug(*args):
    if DEBUG:
        print(
            "task ID:",
            ray.worker.global_worker.current_task_id,
            *args,
            flush=True)

def wait_queue(queue, max_queue_length):
    wait_time = 0
    while len(queue) > max_queue_length:
        _, queue[:-1] = ray.wait(queue[:-1], len(queue) - 1, timeout=0.1, request_once=True)
        wait_time += 0.1

        # Hack to resubmit the last task. If we've waited for a while and
        # there's still no progress, then try a long-standing ray.wait on
        # the last task that we submitted to resubmit it.
        if wait_time > 0.3 and len(queue) > 0:
            last_item = queue[-1]
            ray.wait([last_item], 1, timeout=0)



# A custom data source that reads articles from wikipedia
# Custom data sources need to implement a get_next() method
# that returns the next data element, in this case sentences
@ray.remote
class WordSource(object):
    def __init__(self,
            operator_id,
            handles,
            max_queue_length,
            checkpoint_interval,
            words_file):
        # Titles in this file will be as queries
        self.words_file = words_file

        self.operator_id = operator_id
        self.index = 0
        self.handles = handles
        self.queue = []
        self.max_queue_length = max_queue_length

        # How many checkpoints have been taken so far.
        self.checkpoint_epoch = 0
        # Number of records in an epoch.
        self.checkpoint_interval = checkpoint_interval
        self.records_since_checkpoint = 0
        self.num_records_seen = 0

    # Returns next sentence from the input file
    def generate(self, num_records, batch_size, target_throughput=-1):
        start_time = time.time()
        self.reader = open(self.words_file, 'r')
        i = 0
        handle = self.handles[i]
        while self.num_records_seen < num_records:
            batch = [(time.time(), self.reader.readline()) for _ in range(batch_size)]
            assert(len(batch) > 0), len(batch)

            self.records_since_checkpoint += len(batch)
            if self.records_since_checkpoint >= self.checkpoint_interval:
                self.checkpoint_epoch += 1
                self.records_since_checkpoint -= self.checkpoint_interval
                # TODO: take a checkpoint.

            self.queue.append(handle.push.remote(self.operator_id, batch, self.checkpoint_epoch))
            wait_queue(self.queue, self.max_queue_length)

            self.num_records_seen += len(batch)
            i += 1
            handle = self.handles[(i + 1) % len(self.handles)]

        ray.get([handle.push.remote(self.operator_id, [], self.checkpoint_epoch) for handle in self.handles])
        throughput = self.num_records_seen / (time.time() - start_time)
        return throughput


class NondeterministicOperator(ray.actor.Checkpointable):
    def __init__(self, operator_id, handles, max_queue_length, upstream_ids, checkpoint_dir, batch_size):
        self.operator_id = operator_id
        self.handles = handles
        self._ray_downstream_actors = [handle._ray_actor_id for handle in handles]
        self.num_records_seen = 0

        self.checkpoint_buffer = []
        self.upstream_ids = upstream_ids
        self.checkpoints_pending = set()
        self.checkpoint_epoch = 0
        self._should_checkpoint = False
        self.flush_checkpoint_buffer = False

        # Create the checkpoint directory.
        self.checkpoint_dir = checkpoint_dir
        try:
            os.makedirs(self.checkpoint_dir)
        except FileExistsError:
            pass

        self.batch_size = batch_size
        self.flush_buffers = [list() for _ in range(len(handles))]
        self.num_reducers = len(handles)

        self.queue = []
        self.max_queue_length = max_queue_length

    def ping(self):
        return

    def checkpoint(self, upstream_id, checkpoint_epoch):
        if checkpoint_epoch > self.checkpoint_epoch:
            # This is the first checkpoint marker for the new checkpoint
            # interval that we've received so far.
            if len(self.checkpoints_pending) == 0:
                debug("Starting checkpoint", self.checkpoint_epoch)
                self.checkpoints_pending = set(self.upstream_ids)
            # Record the checkpoint marker received from this upstream actor's
            # operator_id.
            debug("Received checkpoint marker", checkpoint_epoch, "from", upstream_id)
            self.checkpoints_pending.discard(upstream_id)
            # If we've received all checkpoint markers from all upstream
            # actors, then take the checkpoint.
            if len(self.checkpoints_pending) == 0:
                debug("Received all checkpoint markers, taking checkpoint for interval", self.checkpoint_epoch)
                self._should_checkpoint = True
            process_record = False
        else:
            process_record = True
        return process_record

    def push(self, upstream_id, records, checkpoint_epoch):
        if ray.worker.global_worker.task_context.nondeterministic_events is not None:
            submit_log = [int(event.decode('ascii')) for event in ray.worker.global_worker.task_context.nondeterministic_events]
            debug("REPLAY: Submit log", submit_log)
        else:
            submit_log = None

        if self.flush_checkpoint_buffer:
            self.push_checkpoint_buffer(submit_log)
            self.flush_checkpoint_buffer = False

        if submit_log is not None:
            self.replay_push(upstream_id, records, checkpoint_epoch, submit_log)
        else:
            self.log_push(upstream_id, records, checkpoint_epoch)

    def replay_push(self, upstream_id, records, checkpoint_epoch, submit_log):
        process_records = self.checkpoint(upstream_id, checkpoint_epoch)
        debug("REPLAY: process records?", process_records)

        if process_records:
            if len(records) == 0:
                # This is the last batch that we will receive from this
                # upstream operator.
                for i, flush_buffer in enumerate(self.flush_buffers):
                    if len(flush_buffer) > 0:
                        self.flush(i)
                # Send an empty batch. Block on the result to notify the
                # upstream operator when we are finished processing all of its
                # records.
                ray.get([self.flush(i) for i in range(len(self.flush_buffers))])
            else:
                while submit_log and records:
                    next_task_id = ray._raylet.generate_actor_task_id(
                            ray.worker.global_worker.task_driver_id,
                            self.handle._ray_actor_id,
                            self.handle._ray_actor_handle_id,
                            self.handle._ray_actor_counter)
                    task = ray.global_state.task_table(task_id=next_task_id)
                    if not task or task["ExecutionSpec"]["NumExecutions"] < 1:
                        debug("REPLAY: never executed task", next_task_id)
                        break
                    #else:
                    #    assert task["ExecutionSpec"]["NumExecutions"] >= 1, task

                    num_skip_records = submit_log[0] - self.num_records_seen
                    debug("REPLAY: submit:", submit_log[0], " skipping:", num_skip_records, "seen:", self.num_records_seen, "num records:", len(records))
                    assert num_skip_records > 0, (num_skip_records, submit_log, self.num_records_seen)

                    flush = num_skip_records <= len(records)
                    num_records = len(records)
                    #for record in records[:num_skip_records]:
                    #    debug("SKIP RECORD:", upstream_id, record)
                    records = records[num_skip_records:]
                    num_skipped = num_records - len(records)
                    self.num_records_seen += num_skipped

                    if flush:
                        future = self.flush()
                        debug("REPLAY: skipping submit after", self.num_records_seen, future)
                        submit_log.pop(0)
                for record in records:
                    debug("REPLAY RECORD:", upstream_id, record)
                    # Process the record.
                    self.flush_buffer.append(record)
                    self.num_records_seen += 1
                    if len(submit_log) > 0 and submit_log[0] == self.num_records_seen:
                        future = self.flush()
                        debug("REPLAY: submit after", self.num_records_seen, future)
                        submit_log.pop(0)

        else:
            self.checkpoint_buffer.append((upstream_id, records, checkpoint_epoch))


    def log_push(self, upstream_id, records, checkpoint_epoch):
        process_records = self.checkpoint(upstream_id, checkpoint_epoch)
        debug("PUSH: process records?", process_records)

        if process_records:
            if len(records) == 0:
                # This is the last batch that we will receive from this
                # upstream operator.
                for i, flush_buffer in enumerate(self.flush_buffers):
                    if len(flush_buffer) > 0:
                        self.flush(i)
                # Send an empty batch. Block on the result to notify the
                # upstream operator when we are finished processing all of its
                # records.
                ray.get([self.flush(i) for i in range(len(self.flush_buffers))])
            else:
                for record in records:
                    debug("RECORD:", upstream_id, record)
                    records = self.process(record)
                    # Process the record.
                    for key, record in records:
                        self.flush_buffers[key].append(record)
                    self.num_records_seen += 1
                    for i in range(self.num_reducers):
                        self.try_flush(i)

        else:
            self.checkpoint_buffer.append((upstream_id, records, checkpoint_epoch))

    def key(self, word):
        return int(hashlib.md5(word.encode('ascii')).hexdigest(), 16) % self.num_reducers

    def process(self, record):
        timestamp, line = record
        words = line.strip().split(' ')
        return [(self.key(word), (timestamp, word, 1)) for word in words]

    def try_flush(self, buffer_index):
        # Flush randomly.
        do_flush = len(self.flush_buffers[buffer_index]) >= self.batch_size
        # If we are about to take a checkpoint, then force a flush.
        do_flush = do_flush or self._should_checkpoint
        if do_flush:
            #future = self.flush(event=str(self.num_records_seen).encode('ascii'))
            future = self.flush(buffer_index)
            debug("Flushing after", self.num_records_seen, future)
            self.queue.append(future)
            wait_queue(self.queue, self.max_queue_length)

    def flush(self, buffer_index, event=None):
        flush_buffer = self.flush_buffers[buffer_index]
        future = self.handles[buffer_index].push._remote(
                args=[self.operator_id, flush_buffer, self.checkpoint_epoch],
                kwargs={},
                nondeterministic_event=event)
        flush_buffer.clear()
        return future

    def get_pid(self):
        return os.getpid()

    def should_checkpoint(self, checkpoint_context):
        should_checkpoint = self._should_checkpoint
        self._should_checkpoint = False
        return should_checkpoint

    def save_checkpoint(self, actor_id, checkpoint_id):
        debug("Saving checkpoint", self.checkpoint_epoch, checkpoint_id)
        assert len(self.checkpoints_pending) == 0
        checkpoint = {
                "handles": self.handles,
                "checkpoint_id": checkpoint_id,
                "checkpoint_epoch": self.checkpoint_epoch,
                "buffer": self.checkpoint_buffer,
                "num_records_seen": self.num_records_seen,
                }
        checkpoint = pickle.dumps(checkpoint)
        # NOTE: The default behavior is to register a random actor handle
        # whenever a handle is pickled, so that the execution dependency is
        # never removed and anytime the handle is unpickled, we will be able to
        # submit tasks.  However, we do not need to do this since we are only
        # going to unpickle the handle once, when the actor recovers from the
        # checkpoint.
        [handle._ray_new_actor_handles.clear() for handle in self.handles]
        checkpoint_path = 'checkpoint-{}-{}'.format(actor_id.hex(), self.checkpoint_epoch)
        checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_path)
        with open(checkpoint_path, 'wb+') as f:
            f.write(checkpoint)

        self.checkpoint_epoch += 1
        self.flush_checkpoint_buffer = True

    def push_checkpoint_buffer(self, submit_log=None):
        debug("Pushing checkpoint buffer", self.checkpoint_epoch)

        # Make a copy of the checkpoint buffer and try to process them again.
        checkpoint_buffer = self.checkpoint_buffer[:]
        self.checkpoint_buffer.clear()
        if submit_log is not None:
            for upstream_id, records, checkpoint_epoch in checkpoint_buffer:
                self.replay_push(upstream_id, records, checkpoint_epoch, submit_log)
        else:
            for upstream_id, records, checkpoint_epoch in checkpoint_buffer:
                self.log_push(upstream_id, records, checkpoint_epoch)
        debug("Done pushing checkpoint buffer", self.checkpoint_epoch)

    def load_checkpoint(self, actor_id, available_checkpoints):
        debug("Available checkpoints", available_checkpoints)

        # Get the latest checkpoint that completed.
        checkpoint_tracker = named_actors.get_actor("checkpoint_tracker")
        latest_checkpoint_interval = ray.get(checkpoint_tracker.get_current_epoch.remote())
        assert latest_checkpoint_interval > 0, "Actor died before its first checkpoint was taken"
        # Read the latest checkpoint from disk.
        checkpoint_path = 'checkpoint-{}-{}'.format(actor_id.hex(), latest_checkpoint_interval)
        checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_path)
        with open(checkpoint_path, 'rb') as f:
            checkpoint = pickle.loads(f.read())
        self.handles = checkpoint["handles"]
        [handle.reset_handle_id() for handle in self.handles]
        self.num_records_seen = checkpoint["num_records_seen"]

        self.checkpoint_epoch = checkpoint["checkpoint_epoch"]
        assert self.checkpoint_epoch == latest_checkpoint_interval
        self.checkpoint_epoch += 1
        # Try to process the records that were in the buffer.
        self.checkpoint_buffer = checkpoint["buffer"]
        self.flush_checkpoint_buffer = True
        #for upstream_id, record, checkpoint_epoch in checkpoint["buffer"]:
        #    self.replay_push(upstream_id, record, checkpoint_epoch)

        checkpoint_id = checkpoint["checkpoint_id"]
        debug("Reloaded checkpoint", latest_checkpoint_interval, checkpoint_id)
        return checkpoint_id

    def checkpoint_expired(self, actor_id, checkpoint_id):
        return

@ray.remote
class Reducer(NondeterministicOperator):
    def __init__(self, *args):
        super().__init__(*args)
        self.counts = {
                }

    def process(self, record):
        timestamp, word, count = record
        if word not in self.counts:
            self.counts[word] = 0
        self.counts[word] += count
        return [(0, (timestamp, word, self.counts[word]))]

    def get_counts(self):
        return self.counts

class Sink(object):
    def __init__(self, operator_id, source_keys, checkpoint_tracker):
        self.operator_id = operator_id
        self.checkpoint_tracker = checkpoint_tracker
        self.checkpoint_epoch = 0
        self.latencies = []

    def push(self, upstream_id, records, checkpoint_epoch):
        for record in records:
            timestamp, source_key, val = record
            self.latencies.append(time.time() - timestamp)

            # TODO: Normally we would also take a checkpoint on the sink, then
            # release the new checkpoint interval to the tracker.
            if checkpoint_epoch > self.checkpoint_epoch:
                assert checkpoint_epoch == self.checkpoint_epoch + 1, ("Sink did not receive any records for checkpoint", checkpoint_epoch)
                # Notify the checkpoint tracker that we have completed this
                # checkpoint.
                self.checkpoint_tracker.notify_checkpoint_complete.remote(self.operator_id, self.checkpoint_epoch)
                self.checkpoint_epoch = checkpoint_epoch

    def get_latencies(self):
        return self.latencies

@ray.remote(resources={"Node0": 1})
class CheckpointTracker(object):
    def __init__(self, sink_keys):
        self.sink_keys = sink_keys
        self.sinks_pending = set(self.sink_keys)
        self.checkpoint_epoch = -1

    def notify_checkpoint_complete(self, sink_key, checkpoint_epoch):
        assert checkpoint_epoch == self.checkpoint_epoch + 1

        self.sinks_pending.remove(sink_key)
        # If we have received the checkpoint interval from all sinks, then the
        # checkpoint is complete.
        if len(self.sinks_pending) == 0:
            self.checkpoint_epoch += 1
            self.sinks_pending = set(self.sink_keys)

    def get_current_epoch(self):
        return self.checkpoint_epoch


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Benchmarks.')
    parser.add_argument(
        '--num-workers',
        default=1,
        type=int,
        help='The number of intermediate, nondeterministic operators to use.')
    parser.add_argument(
        '--checkpoint-interval',
        default=10000,
        type=int,
        help='The number of records to process per source in one checkpoint epoch.')
    parser.add_argument(
        '--same-node',
        action='store_true',
        help='Place all intermediate operators on the same node.')
    parser.add_argument(
        '--words-file',
        type=str,
        required=True,
        help='Words file')
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size')
    parser.add_argument(
        '--num-records',
        type=int,
        default=200000,
        help='Number of records to generate')
    parser.add_argument(
        '--max-queue-length',
        type=int,
        default=10,
        help='Queue length')
    parser.add_argument(
        '--flush-probability',
        type=float,
        default=1.0,
        help='The probability of flushing a batch on the nondeterministic operator.')
    args = parser.parse_args()

    # Create the checkpoint directory.
    checkpoint_dir = os.path.join(
        CHECKPOINT_DIR, ray.worker.global_worker.task_driver_id.hex())
    try:
        os.makedirs(checkpoint_dir)
    except FileExistsError:
        pass


    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 20,
        "object_manager_repeated_push_delay_ms": 1000,
        "object_manager_pull_timeout_ms": 1000,
        "gcs_delay_ms": 100,
        # We will kill all nondeterministic workers, so make sure we can
        # tolerate that many failures.
        "lineage_stash_max_failures": args.num_workers,
        "node_manager_forward_task_retry_timeout_milliseconds": 100,
    })

    node_kwargs = {
        "num_cpus": 4,
        "object_store_memory": 10**9,
        "_internal_config": internal_config,
    }

    cluster = Cluster(initialize_head=True, head_node_args=node_kwargs)
    if args.same_node:
        # One node for all nondeterministic operators, plus one for the source
        # operators, plus one for the sink operators.
        num_nodes = 1 + 1 + 1
    else:
        # One node for each nondeterministic operator, plus one for the source
        # operators, plus one for the sink operators.
        num_nodes = args.num_workers + 1 + 1
    resources = []
    nodes = []
    for i in range(num_nodes):
        resource = "Node{}".format(i)
        node_kwargs["resources"] = {resource: 100}
        resources.append(resource)
        nodes.append(cluster.add_node(**node_kwargs))
    redis_address = cluster.redis_address
    ray.init(redis_address=redis_address)

    num_sources = 1
    operator_ids = list(string.ascii_uppercase)
    source_keys = [operator_ids.pop(0) for _ in range(num_sources)]
    intermediate_keys = [operator_ids.pop(0) for _ in range(args.num_workers)]
    reducer_key = operator_ids.pop(0)
    # One sink.
    sink_key = operator_ids.pop(0)

    checkpoint_tracker = CheckpointTracker.remote([sink_key])
    named_actors.register_actor("checkpoint_tracker", checkpoint_tracker)

    # Create the sink.
    sink_cls = ray.remote(resources={
            resources[-1]: 1
            })(Sink)
    sink = sink_cls.remote(sink_key, source_keys, checkpoint_tracker)

    upstream_keys = intermediate_keys
    reducer = Reducer.remote(reducer_key, [sink], args.max_queue_length, upstream_keys, checkpoint_dir, args.batch_size)

    # Create the intermediate operators.
    operators = []
    operator = reducer
    for i, key in reversed(list(enumerate(intermediate_keys))):
        if args.same_node:
            # All nondeterministic operators are placed on the same node.
            resource = resources[1]
        else:
            # Each nondeterministic operator is placed on a different node.
            resource = resources[i + 1]

        cls = ray.remote(resources={
            resource: 1,
            }, max_reconstructions=100)(NondeterministicOperator)
        if i == 0:
            upstream_keys = source_keys
        else:
            upstream_keys = [intermediate_keys[i-1]]
        print("Starting intermediate operator", key, upstream_keys)
        operator = cls.remote(key, [operator], args.max_queue_length, upstream_keys, checkpoint_dir, args.batch_size)
        operators.append(operator)

    ray.get([operator.ping.remote() for operator in operators])


    # Create the sources.
    source_args = [None, operators, args.max_queue_length, args.checkpoint_interval, args.words_file]
    sources = []
    for key in source_keys:
        source_args[0] = key
        sources.append(WordSource._remote(args=source_args, kwargs={}, resources={"Node0": 1}))

    start = time.time()
    generators = [source.generate.remote(args.num_records, args.batch_size) for source in sources]

    time.sleep(2)
    # The intermediate operators are on all nodes except the source node and
    # the sink node.
    intermediate_nodes = nodes[1:-1]
    intermediate_resources = resources[1:-1]
    # Kill and restart all intermediate operators.
    #for node in intermediate_nodes:
    #    cluster.remove_node(node)
    #for resource in intermediate_resources:
    #    node_kwargs["resources"] = {resource: 100}
    #    cluster.add_node(**node_kwargs)

    throughputs = ray.get(generators)
    end = time.time()
    print("Elapsed time:", end - start)
    print("Source throughputs:", throughputs)
    latencies = ray.get(sink.get_latencies.remote())
    print("Mean latency:", np.mean(latencies), "max latency:", np.max(latencies))

    counts = ray.get(reducer.get_counts.remote())
    counts_200000 = {'hello': 200000, 'bye': 66666, 'there': 133334, 'hi': 66667, 'good': 66666}
    print("Final count is", counts)
    for key, value in counts.items():
        assert counts_200000[key] == value
    for key, value in counts_200000.items():
        assert counts[key] == value

    events = ray.global_state.chrome_tracing_dump()
    with open("test.json", "w") as outfile:
        json.dump(events, outfile)
