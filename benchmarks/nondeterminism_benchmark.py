import json
import time
import os

import ray
from ray.tests.cluster_utils import Cluster
import ray.cloudpickle as pickle


@ray.remote(resources={"Node0": 1})
class Source(object):
    def __init__(self, key, handle, max_queue_length):
        self.key = key
        self.index = 0
        self.handle = handle
        self.queue = []
        self.max_queue_length = max_queue_length

    def add_handle(self, handle):
        self.handle = handle

    def generate(self, num_records):
        for i in range(num_records):
            self.push()
        ray.get(self.handle.push.remote(None))

    def push(self):
        record = (self.key, self.index)
        self.queue.append(self.handle.push.remote(record))
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
    def __init__(self, handle):
        self.handle = handle
        self.iterations = 0
        self._ray_downstream_actors = [handle._ray_actor_id]

    def push(self, record):
        done = self.handle.push.remote(record)
        if record is None:
            ray.get(done)
        self.iterations += 1

    def get_pid(self):
        return os.getpid()

    def should_checkpoint(self, checkpoint_context):
        if self.iterations > 0 and self.iterations % 100 == 0:
            return True
        else:
            return False

    def save_checkpoint(self, actor_id, checkpoint_id):
        print("Saving checkpoint", self.iterations, checkpoint_id)
        checkpoint = {
                "handle": self.handle,
                "iterations": self.iterations,
                "checkpoint_id": checkpoint_id,
                }
        checkpoint = pickle.dumps(checkpoint)
        print("new actor handles", self.handle._ray_new_actor_handles)
        self.handle._ray_new_actor_handles.clear()
        with open('/home/stephanie/ray-fork/benchmarks/checkpoint-{}'.format(actor_id.hex()), 'wb+') as f:
            f.write(checkpoint)

    def load_checkpoint(self, actor_id, available_checkpoints):
        print("Available checkpoints", available_checkpoints)
        with open('/home/stephanie/ray-fork/benchmarks/checkpoint-{}'.format(actor_id.hex()), 'rb') as f:
            checkpoint = pickle.loads(f.read())
        self.handle = checkpoint["handle"]
        self.iterations = checkpoint["iterations"]
        self.handle.reset_handle_id()
        checkpoint_id = checkpoint["checkpoint_id"]
        print("Reloaded checkpoint", self.iterations, checkpoint_id, flush=True)
        assert checkpoint_id == available_checkpoints[0].checkpoint_id
        return checkpoint_id

    def checkpoint_expired(self, actor_id, checkpoint_id):
        return

@ray.remote(resources={"Node3": 1})
class Sink(object):
    def __init__(self, keys):
        self.records = {
                key: 0 for key in keys
                }

    def push(self, record):
        if record is None:
            return
        key, val = record
        assert self.records[key] == val, (key, self.records[key], val)
        self.records[key] += 1

    def get_records(self):
        return self.records
                

if __name__ == '__main__':
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 20,
        "object_manager_repeated_push_delay_ms": 1000,
        "object_manager_pull_timeout_ms": 1000,
        "gcs_delay_ms": 100,
        "lineage_stash_max_failures": 2,
        "node_manager_forward_task_retry_timeout_milliseconds": 100,
    })

    node_kwargs = {
        "num_cpus": 4,
        "object_store_memory": 10**9,
        "_internal_config": internal_config,
    }
    cluster = Cluster(initialize_head=True, head_node_args=node_kwargs)
    num_workers = 4
    for i in range(num_workers):
        node_kwargs["resources"] = {"Node{}".format(i): 100}
        cluster.add_node(**node_kwargs)
    redis_address = cluster.redis_address

    ray.init(redis_address=redis_address)

    source_keys = ["a", "b", "c"]
    max_queue_length = 100
    sink = Sink.remote(source_keys)
    cls1 = ray.remote(resources={"Node1": 1}, max_reconstructions=100)(NondeterministicOperator)
    nondeterministic_operator1 = cls1.remote(sink)
    # Modify this resource to be Node2 to test case where multiple actors on same machine fail.
    cls2 = ray.remote(resources={"Node2": 1}, max_reconstructions=100)(NondeterministicOperator)
    nondeterministic_operator2 = cls2.remote(nondeterministic_operator1)
    sources = [Source.remote(key, nondeterministic_operator2, max_queue_length) for key in source_keys]

    start = time.time()
    num_records = 1000
    generators = [source.generate.remote(num_records) for source in sources]

    time.sleep(3)
    node = cluster.list_all_nodes()[-2]
    cluster.remove_node(node)
    node = cluster.list_all_nodes()[-2]
    cluster.remove_node(node)
    node_kwargs["resources"] = {"Node1": 100}
    cluster.add_node(**node_kwargs)
    node_kwargs["resources"] = {"Node2": 100}
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
