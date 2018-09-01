from __future__ import print_function
import argparse
import ray
import ray.cloudpickle as pickle
import time
import logging
import numpy as np
import ujson

from conf import *
from generate import *
from ysb_stream_tasks import *

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

def ray_warmup(node_resources, *dependencies):
    """
    Warmup Ray
    """
    warmups = []
    for node_resource in node_resources:
        warmups.append(warmup._submit(
            args=dependencies, 
            resources={node_resource: 1}))
    ray.get(warmups)

def submit_tasks():
    generated = []
    for i in range(num_nodes):
        for _ in range(num_generators_per_node):
            generated.append(generate._submit(
                args=[num_generator_out] + list(gen_deps) + [time_slice_num_events],
                num_return_vals=num_generator_out, 
                resources={node_resources[i]: 1}))
    generated = flatten(generated)

    parsed, start_idx = [], 0
    for i in range(num_nodes):
        for _ in range(num_parsers_per_node):
            parsed.append(parse_json._submit(
                args=[num_parser_out] + generated[start_idx : start_idx + num_parser_in],
                num_return_vals=num_parser_out,
                resources={node_resources[i] : 1}))
            start_idx += num_parser_in
    parsed = flatten(parsed)

    filtered, start_idx = [], 0
    for i in range(num_nodes):
        for _ in range(num_filters_per_node):
            filtered.append(filter._submit(
                args=[num_filter_out] + parsed[start_idx : start_idx + num_filter_in],
                num_return_vals=num_filter_out,
                resources={node_resources[i] : 1}))
            start_idx += num_filter_in 
    filtered = flatten(filtered)

    shuffled, start_idx = [], 0
    for i in range(num_nodes):
        for _ in range(num_projectors_per_node):
            batches = filtered[start_idx : start_idx + num_projector_in]
            shuffled.append(project_shuffle._submit(
                args=[num_projector_out] + batches,
                num_return_vals=num_projector_out,
                resources={node_resources[i] : 1}))
            start_idx += num_projector_in
    shuffled = np.array(shuffled)

    if num_reducers == 1:
        # Handle return type difference for num_ret_vals=1
        shuffled = np.reshape(shuffled, (len(shuffled), 1))

    [reducers[i].reduce.remote(*shuffled[:,i]) for i in range(num_reducers)]

def compute_stats():
    i, total = 0, 0
    missed_total, miss_time_total = 0, 0
    for reducer in reducers:
        reducer_total = 0
        i += 1
        print("Counted:", ray.get(reducer.count.remote()))
        seen = ray.get(reducer.seen.remote())
        for key in seen:
            if key[1] < end_time:
                reducer_total += seen[key]
            else:
                missed_total += seen[key]
                miss_time_total += (end_time - key[1])
		#print("Missed by:", )
        total += reducer_total
        print("Seen by reducer", i, ": ", reducer_total)

    thput = 3 * total/exp_time
    miss_thput = 3 * missed_total/exp_time
    avg_miss_lat = miss_time_total / missed_total if missed_total != 0 else 0
    print("System Throughput: ", thput, "events/s")
    print("Missed: ", miss_thput, "events/s by", avg_miss_lat, "on average.")

def compute_checkpoint_overhead():
    checkpoints = ray.get([reducer.__ray_save__.remote() for reducer in reducers])
    start = time.time()
    pickles = [ray.cloudpickle.dumps(checkpoint) for checkpoint in checkpoints]
    end = time.time()
    print("Checkpoint pickle time", end - start)
    print("Checkpoint size(bytes):", [len(pickle) for pickle in pickles])

def write_latencies():
    f = open("lat.txt", "w+")
    lat_total, count = 0, 0
    mid_time = (end_time - start_time) / 2
    for reducer in reducers:
        lats = ray.get(reducer.get_latencies.remote())
        for key in lats:
            if key[1] < end_time:
                f.write(str(lats[key]) + "\n")
                lat_total += lats[key]
                count += 1
    print("Average latency: ", lat_total/count)
    f.close()

def write_timeseries():
    f = open("timeseries.txt", "w+")
    total_lat = defaultdict(int)
    total_count = defaultdict(int)
    for reducer in reducers:
        lats = ray.get(reducer.get_latencies.remote())
        for key in lats:
            if key[1] < end_time:
                total_lat[key[1]] += lats[key]
                total_count[key[1]] += 1
    for key in total_lat:
        f.write(str(key) + " " + str(total_lat[key]/total_count[key]) + "\n")
    f.close()

def get_node_names(num_nodes):
    node_names = set()
    while len(node_names) < num_nodes:
        hosts = [ping.remote() for _ in range(num_nodes * 100)]
        hosts, incomplete = ray.wait(hosts, timeout=30000) # timeout after 10s
        [node_names.add(ray.get(host_id)) for host_id in hosts]
	print(len(hosts), len(node_names))
        print("Nodes:", node_names)
	if incomplete:
            print("Timed-out after getting: ", len(hosts), "and missing", len(incomplete))
    return list(node_names)

def read_node_names(num_nodes):
    with open('/home/ubuntu/swang-ray/benchmarks/stream/conf/priv-hosts-all') as f:
        lines = f.readlines()
    names = [l.strip() for l in lines][:num_nodes]
    if len(names) < num_nodes:
        raise IOError("File contains less than the requested number of nodes")
    return names

def ping_node(node_name):
    name = ray.get(ping._submit(args=[0.1], resources={node_name:1}))
    if name != node_name:
        print("Tried to ping", node_name, "but got", name)
    else:
        print("Pinged", node_name)
    return name

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump', type=str, default='dump.json')
    parser.add_argument('--exp-time', type=int, default=60)
    parser.add_argument('--no-hugepages', action='store_true')
    parser.add_argument('--actor-checkpointing', action='store_true')
    parser.add_argument('--num-projectors', type=int, default=1)
    parser.add_argument('--num-filters', type=int, default=1)
    parser.add_argument('--num-generators', type=int, default=1)
    parser.add_argument('--num-nodes', type=int, required=True)
    parser.add_argument('--num-parsers', type=int, default=1)
    parser.add_argument('--num-reducers', type=int, default=1)
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--target-throughput', type=int, default=1e5)
    parser.add_argument('--test-throughput', action='store_true')
    parser.add_argument('--time-slice-ms', type=int, default=100)
    parser.add_argument('--warmup-time', type=int, default=60)
    args = parser.parse_args()

    checkpoint = args.actor_checkpointing
    exp_time = args.exp_time
    warmup_time = args.warmup_time
    num_nodes = args.num_nodes
    time_slice_num_events = int(args.target_throughput * BATCH_SIZE_SEC)

    num_generators_per_node = args.num_generators
    num_parsers_per_node = args.num_parsers 
    num_filters_per_node = args.num_filters
    num_projectors_per_node = args.num_projectors 
    num_reducers_per_node = args.num_reducers 

    num_generators = args.num_generators * num_nodes
    num_parsers = args.num_parsers * num_nodes
    num_filters = args.num_filters * num_nodes
    num_projectors = args.num_projectors * num_nodes
    num_reducers = args.num_reducers * num_nodes

    num_generator_out = max(1, num_parsers // num_generators)
    num_parser_in = max(1, num_generators // num_parsers)
    num_parser_out = max(1, num_filters // num_parsers)
    num_filter_in = max(1, num_parsers // num_filters)
    num_filter_out = max(1, num_projectors  // num_filters)
    num_projector_in = max(1, num_filters_per_node * num_filter_out // num_projectors_per_node)
    num_projector_out = num_reducers

    print("Per node setup: ")
    print("[", num_generators_per_node, "generators ] -->", num_generator_out)
    print(num_parser_in, "--> [", num_parsers_per_node, "parsers ] -->", num_parser_out  )
    print(num_filter_in, "--> [", num_filters_per_node, "filters ] -->", num_filter_out  )
    print(num_projector_in, "--> [", num_projectors_per_node, "projectors ] -->", num_projector_out)
    print(num_projectors, "--> [", num_reducers_per_node, "reducers]")

    if args.redis_address is None:
        node_resources = ["Node{}".format(i) for i in range(1, num_nodes + 1)]
        huge_pages = not args.no_hugepages
        plasma_directory = "/mnt/hugepages" if huge_pages else None
        resources = [dict([(node_resource, num_generators + num_projectors + 
                            num_parsers + num_filters + num_reducers)]) 
                    for node_resource in node_resources]
        ray.worker._init(
            start_ray_local=True,
            redirect_output=True,
            use_raylet=True,
            resources=resources,
            num_local_schedulers=num_nodes,
            huge_pages=huge_pages,
            plasma_directory=plasma_directory)
    else:
        ray.init(redis_address=args.redis_address, use_raylet=True)
        print("Discovering nodes...")
        #node_resources = get_node_names(num_nodes)
        #node_resources = [args.redis_address] + read_node_names(num_nodes - 1)
        node_resources = read_node_names(num_nodes)
        print("Discovered", len(node_resources), "resources:", node_resources)
	[ping_node(node) for node in node_resources]
    time.sleep(2)

    print("Initializing generators...")
    gen_deps = init_generator(AD_TO_CAMPAIGN_MAP, time_slice_num_events)
    print("Initializing reducers...")
    reducers = [init_actor(i, node_resources, Reducer, checkpoint) for i in range(num_reducers)]
    # Make sure the reducers are initialized.
    ray.get([reducer.clear.remote() for reducer in reducers])
    print("Placing dependencies on nodes...")
    ray_warmup(node_resources, gen_deps)

    try:
        time_to_sleep = BATCH_SIZE_SEC

        print("Warming up...")
        start_time = time.time()
        end_time = start_time + warmup_time

        while time.time() < end_time:
            submit_tasks()
            time.sleep(time_to_sleep)

        print("Clearing reducers...")
        time.sleep(5) # TODO non-deterministic, fix
        ray.wait([reducer.clear.remote() for reducer in reducers])

        print("Measuring...")
        start_time = time.time()
        end_time = start_time + exp_time

        while time.time() < end_time:
            loop_start = time.time()
            submit_tasks()
            loop_end = time.time()
            time_to_sleep = BATCH_SIZE_SEC - (loop_end - loop_start)
            if time_to_sleep > 0:
                time.sleep(time_to_sleep) 

        end_time = time.time()

        print("Finished in: ", (end_time - start_time), "s")

        print("Computing throughput...")
        compute_stats() 
    
        print("Dumping...")
        ray.global_state.chrome_tracing_dump(filename=args.dump)
    
        print("Writing latencies...")
        write_latencies()
    
        print("Computing checkpoint overhead...")
        compute_checkpoint_overhead()
    
        print("Writing timeseries...")
        write_timeseries()
    
    except KeyboardInterrupt: 
        print("Dumping current state...")
        ray.global_state.chrome_tracing_dump(filename=args.dump)
        exit()

