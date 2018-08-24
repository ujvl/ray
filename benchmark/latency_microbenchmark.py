from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import datetime
import time
import argparse
import numpy as np

BATCH_SIZE = 1000
NUM_ITEMS = 10 * 1000

class A(object):
    def __init__(self, node_resource):
        print("Actor A start...")
        self.b = ray.remote(resources={node_resource: 1})(B).remote()
        ray.get(self.b.ready.remote())

    def ready(self):
        time.sleep(1)
        return True

    def f(self):
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push, start " + time_str)
        latencies = []
        for i in range(0, NUM_ITEMS, BATCH_SIZE):
            start = time.time()
            for _ in range(0, BATCH_SIZE):
                ray.get(self.b.f.remote())
            end = time.time()
            latency = end - start
            print("push, latency round ", i , ":", latency / BATCH_SIZE)
            latencies.append(latency)
        return latencies

class B(object):
    def __init__(self):
        print("Actor B start...")
        self.sum = 0
        #import yep
        #yep.start('actorB.prof')

    def ready(self):
        time.sleep(1)
        return True

    def f(self):
        self.sum += 1
        return 1


    def get_sum(self):
        #import yep
        #yep.stop()
        return self.sum


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--target-throughput', type=int, default=3000)
    parser.add_argument('--num-workers', type=int, default=1)
    parser.add_argument('--num-raylets', type=int, default=1)
    parser.add_argument('--num-shards', type=int, default=None)
    parser.add_argument('--gcs', action='store_true')
    parser.add_argument('--policy', type=int, default=0)
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--max-lineage-size', type=str)
    args = parser.parse_args()

    gcs_delay_ms = -1
    if args.gcs:
        gcs_delay_ms = 0

    if args.redis_address is None:
        ray.worker._init(start_ray_local=True, use_raylet=True, num_local_schedulers=args.num_raylets * 2,
                         resources=[
                            {
                                "Node{}".format(i): args.num_workers,
                            } for i in range(args.num_raylets * 2)],
                         gcs_delay_ms=gcs_delay_ms,
                         num_redis_shards=args.num_shards,
                         lineage_cache_policy=args.policy,
                         num_workers=args.num_workers,
                         max_lineage_size=args.max_lineage_size)
    else:
        ray.init(redis_address=args.redis_address + ":6379", use_raylet=True)

    actors = []
    for node in range(args.num_raylets):
        for _ in range(args.num_workers):
            actor_cls = ray.remote(resources={
                "Node{}".format(node * 2): 1,
                })(A)
            actors.append(actor_cls.remote("Node{}".format(node * 2 + 1)))
    total_time = ray.get([actor.ready.remote() for actor in actors])
    latencies = ray.get([actor.f.remote() for actor in actors])
    latencies = [latency[len(latency) // 4 : 3 * len(latency) // 4] for latency in latencies]
    for latency in latencies:
        print("DONE, average latency:", sum(latency) / len(latency) / BATCH_SIZE * 1000000, "us, variance:", np.var(latency))
