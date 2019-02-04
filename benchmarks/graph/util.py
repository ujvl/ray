import argparse
import logging
import time

import ray

import conf


def init_ray(args, node_resources):
    if args.redis_address:
        ray.init(redis_address="{}:6379".format(args.redis_address))
    else:
        plasma_directory = "/mnt/hugepages" if args.huge_pages else None
        resources = {node_resource : 512 for node_resource in node_resources}
        ray.init(
            redirect_output=True,
            resources=resources,
            huge_pages=args.huge_pages is not None,
            plasma_directory=plasma_directory,
        )
    time.sleep(1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump', type=str, default=None)
    parser.add_argument('--graph-fname', type=str, default=None)
    parser.add_argument('--huge-pages', type=str)
    parser.add_argument('--num-nodes', type=int, required=True)
    parser.add_argument('--num-subgraphs', type=int, required=True)
    parser.add_argument('--out-fname', type=str, default="test")
    parser.add_argument('--redis-address', type=str)
    args = parser.parse_args()
    return args


def get_logger(name):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name)
    return logger

def flatten(lst):
 return [item for sublist in lst for item in sublist]


