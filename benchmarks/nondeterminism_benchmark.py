import argparse
import json
import time
import os
from collections import defaultdict
import string

import ray
from ray.tests.cluster_utils import Cluster
import ray.cloudpickle as pickle
from ray.experimental import named_actors


CHECKPOINT_DIR = '/tmp/ray-checkpoints'


@ray.remote(resources={"Node0": 1})
class Source(object):
    def __init__(self, operator_id, handle, max_queue_length, checkpoint_interval):
        self.operator_id = operator_id
        self.index = 0
        self.handle = handle
        self.queue = []
        self.max_queue_length = max_queue_length

        # How many checkpoints have been taken so far.
        self.checkpoint_epoch = 0
        # Number of records in an epoch.
        self.checkpoint_interval = checkpoint_interval

    def add_handle(self, handle):
        self.handle = handle

    def generate(self, num_records):
        for i in range(num_records):
            self.push()
        ray.get(self.handle.push.remote(None, self.checkpoint_epoch))

    def push(self):
        record = (self.operator_id, self.operator_id, self.index)
        if self.index % self.checkpoint_interval == 0 and self.index > 0:
            self.checkpoint_epoch += 1
        self.queue.append(self.handle.push.remote(record, self.checkpoint_epoch))
        self.index += 1

        wait_time = 0
        while len(self.queue) > self.max_queue_length:
            _, self.queue[:-1] = ray.wait(self.queue[:-1], len(self.queue) - 1, timeout=0.1, request_once=True)
            wait_time += 0.1

            # Hack to resubmit the last task. If we've waited for a while and
            # there's still no progress, then try a long-standing ray.wait on
            # the last task that we submitted to resubmit it.
            if wait_time > 0.3 and len(self.queue) > 0:
                last_item = self.queue[-1]
                ray.wait([last_item], 1, timeout=0)
                wait_time = 0

class NondeterministicOperator(ray.actor.Checkpointable):
    def __init__(self, operator_id, handle, upstream_ids, checkpoint_dir):
        self.operator_id = operator_id
        self.handle = handle
        self._ray_downstream_actors = [handle._ray_actor_id]

        self.checkpoint_buffer = []
        self.upstream_ids = upstream_ids
        self.checkpoints_pending = set()
        self.checkpoint_epoch = 0
        self._should_checkpoint = False

        # Create the checkpoint directory.
        self.checkpoint_dir = checkpoint_dir
        try:
            os.makedirs(self.checkpoint_dir)
        except FileExistsError:
            pass


    def checkpoint(self, upstream_id, checkpoint_epoch):
        if checkpoint_epoch > self.checkpoint_epoch:
            # This is the first checkpoint marker for the new checkpoint
            # interval that we've received so far.
            if len(self.checkpoints_pending) == 0:
                print("Starting checkpoint", self.checkpoint_epoch)
                self.checkpoints_pending = set(self.upstream_ids)
            # Record the checkpoint marker received from this upstream actor's
            # operator_id.
            print("Received checkpoint marker", checkpoint_epoch, "from", upstream_id)
            self.checkpoints_pending.discard(upstream_id)
            # If we've received all checkpoint markers from all upstream
            # actors, then take the checkpoint.
            if len(self.checkpoints_pending) == 0:
                print("Received all checkpoint markers, taking checkpoint for interval", self.checkpoint_epoch)
                self._should_checkpoint = True
            process_record = False
        else:
            process_record = True
        return process_record

    def push(self, record, checkpoint_epoch):
        process_record = True
        if record is not None:
            upstream_id, _, _ = record
            process_record = self.checkpoint(upstream_id, checkpoint_epoch)

        if process_record:
            if record is not None:
                record = (self.operator_id, record[1], record[2])
            done = self.handle.push.remote(record, checkpoint_epoch)
            if record is None:
                ray.get(done)
        else:
            self.checkpoint_buffer.append((record, checkpoint_epoch))

    def get_pid(self):
        return os.getpid()

    def should_checkpoint(self, checkpoint_context):
        should_checkpoint = self._should_checkpoint
        self._should_checkpoint = False
        return should_checkpoint

    def save_checkpoint(self, actor_id, checkpoint_id):
        print("Saving checkpoint", self.checkpoint_epoch, checkpoint_id)
        assert len(self.checkpoints_pending) == 0
        checkpoint = {
                "handle": self.handle,
                "checkpoint_id": checkpoint_id,
                "checkpoint_epoch": self.checkpoint_epoch,
                "buffer": self.checkpoint_buffer,
                }
        checkpoint = pickle.dumps(checkpoint)
        # NOTE: The default behavior is to register a random actor handle
        # whenever a handle is pickled, so that the execution dependency is
        # never removed and anytime the handle is unpickled, we will be able to
        # submit tasks.  However, we do not need to do this since we are only
        # going to unpickle the handle once, when the actor recovers from the
        # checkpoint.
        self.handle._ray_new_actor_handles.clear()
        checkpoint_path = 'checkpoint-{}-{}'.format(actor_id.hex(), self.checkpoint_epoch)
        checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_path)
        with open(checkpoint_path, 'wb+') as f:
            f.write(checkpoint)

        self.checkpoint_epoch += 1
        # Make a copy of the checkpoint buffer and try to process them again.
        checkpoint_buffer = self.checkpoint_buffer[:]
        self.checkpoint_buffer.clear()
        for record, checkpoint_epoch in checkpoint_buffer:
            self.push(record, checkpoint_epoch)

    def load_checkpoint(self, actor_id, available_checkpoints):
        print("Available checkpoints", available_checkpoints)

        # Get the latest checkpoint that completed.
        checkpoint_tracker = named_actors.get_actor("checkpoint_tracker")
        latest_checkpoint_interval = ray.get(checkpoint_tracker.get_current_epoch.remote())
        assert latest_checkpoint_interval > 0, "Actor died before its first checkpoint was taken"
        # Read the latest checkpoint from disk.
        checkpoint_path = 'checkpoint-{}-{}'.format(actor_id.hex(), latest_checkpoint_interval)
        checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_path)
        with open(checkpoint_path, 'rb') as f:
            checkpoint = pickle.loads(f.read())
        self.handle = checkpoint["handle"]
        self.handle.reset_handle_id()

        self.checkpoint_epoch = checkpoint["checkpoint_epoch"]
        assert self.checkpoint_epoch == latest_checkpoint_interval
        self.checkpoint_epoch += 1
        # Try to process the records that were in the buffer.
        for record, checkpoint_epoch in checkpoint["buffer"]:
            self.push(record, checkpoint_epoch)

        checkpoint_id = checkpoint["checkpoint_id"]
        print("Reloaded checkpoint", latest_checkpoint_interval, checkpoint_id, flush=True)
        return checkpoint_id

    def checkpoint_expired(self, actor_id, checkpoint_id):
        return

class Sink(object):
    def __init__(self, operator_id, source_keys, checkpoint_tracker):
        self.operator_id = operator_id
        self.records = {
                key: 0 for key in source_keys
                }
        self.checkpoint_tracker = checkpoint_tracker
        self.checkpoint_epoch = 0

    def push(self, record, checkpoint_epoch):
        if record is None:
            return
        _, source_key, val = record
        assert self.records[source_key] == val, (source_key, self.records[source_key], val)
        self.records[source_key] += 1

        # TODO: Normally we would also take a checkpoint on the sink, then
        # release the new checkpoint interval to the tracker.
        if checkpoint_epoch > self.checkpoint_epoch:
            assert checkpoint_epoch == self.checkpoint_epoch + 1, ("Sink did not receive any records for checkpoint", checkpoint_epoch)
            # Notify the checkpoint tracker that we have completed this
            # checkpoint.
            self.checkpoint_tracker.notify_checkpoint_complete.remote(self.operator_id, self.checkpoint_epoch)
            self.checkpoint_epoch = checkpoint_epoch

    def get_records(self):
        return self.records
                
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
        default=100,
        type=int,
        help='The number of records to process per source in one checkpoint epoch.')
    parser.add_argument(
        '--same-node',
        action='store_true',
        help='Place all intermediate operators on the same node.')
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

    max_queue_length = 100
    num_sources = 3
    operator_ids = list(string.ascii_uppercase)
    source_keys = [operator_ids.pop(0) for _ in range(num_sources)]
    intermediate_keys = [operator_ids.pop(0) for _ in range(args.num_workers)]
    # One sink.
    sink_key = operator_ids.pop(0)

    checkpoint_tracker = CheckpointTracker.remote([sink_key])
    named_actors.register_actor("checkpoint_tracker", checkpoint_tracker)

    # Create the sink.
    sink_cls = ray.remote(resources={
            resources[-1]: 1
            })(Sink)
    sink = sink_cls.remote(sink_key, source_keys, checkpoint_tracker)

    # Create the intermediate operators.
    operator = sink
    operators = []
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
        operator = cls.remote(key, operator, upstream_keys, checkpoint_dir)
        operators.append(operator)

    # Create the sources.
    sources = [Source.remote(key, operator, max_queue_length, args.checkpoint_interval) for key in source_keys]

    start = time.time()
    num_records = 1000
    generators = [source.generate.remote(num_records) for source in sources]

    time.sleep(3)
    # The intermediate operators are on all nodes except the source node and
    # the sink node.
    intermediate_nodes = nodes[1:-1]
    intermediate_resources = resources[1:-1]
    # Kill and restart all intermediate operators.
    for node in intermediate_nodes:
        cluster.remove_node(node)
    for resource in intermediate_resources:
        node_kwargs["resources"] = {resource: 100}
        cluster.add_node(**node_kwargs)

    ray.get(generators)
    end = time.time()
    final_records = ray.get(sink.get_records.remote())
    print("Final records:", final_records)
    print("Latency:", end - start)
    assert all([val == 1000 for val in final_records.values()])

    events = ray.global_state.chrome_tracing_dump()
    with open("test.json", "w") as outfile:
        json.dump(events, outfile)
