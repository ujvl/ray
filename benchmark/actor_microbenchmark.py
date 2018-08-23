from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import datetime
import time
import argparse

BATCH_SIZE = 100
NUM_ITEMS = 200 * 1000

class A(object):
    def __init__(self, node_resource):
        print("Actor A start...")
        self.b = ray.remote(resources={node_resource: 1})(B).remote()
        ray.get(self.b.ready.remote())

    def ready(self):
        time.sleep(1)
        return True

    def f(self, target_throughput):
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push, start " + time_str)
        start = time.time()
        start1 = time.time()
        start2 = time.time()
        for i in range(NUM_ITEMS):
            self.b.f.remote(i)
            if i % BATCH_SIZE == 0 and i > 0:
                end = time.time()
                sleep_time = (BATCH_SIZE / target_throughput) - (end - start2)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                start2 = time.time()
            if i % 10000 == 0 and i > 0:
                end = time.time()
                print("push, throughput round ", i / 10000, ":", 10000 / (end - start1))
                start1 = end
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
        print("push, end " + time_str)

        ray.get(self.b.get_sum.remote())
        end = time.time()
        return end - start

class B(object):
    def __init__(self):
        print("Actor B start...")
        self.sum = 0
        #import yep
        #yep.start('actorB.prof')

    def ready(self):
        time.sleep(1)
        return True

    def f(self, object_id):
        #ray.get(object_id)
        #print (object_id)
        self.sum += 1
        if (self.sum == 1) :
            time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
            print("read, start " + time_str)

        if (self.sum == NUM_ITEMS) :
            time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
            print("read, end " + time_str)
        if (self.sum % 10000 == 0) :
            time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')
            print("read " + str(self.sum) + "  " + str(time_str))


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
                         lineage_cache_policy=args.policy)
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
    total_time = ray.get([actor.f.remote(args.target_throughput) for actor in actors])
    throughput = NUM_ITEMS * len(actors) / max(total_time)
    latency = max(total_time) / NUM_ITEMS
    print("DONE, target throughput:", args.target_throughput * len(actors), "total throughput:", throughput, "latency:",  latency)
