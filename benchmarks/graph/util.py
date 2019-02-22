import argparse
import logging
import time

import ray

import conf


default_log_level = logging.DEBUG if conf.DEBUG else logging.INFO


def get_logger(name, level=default_log_level):
    logging.basicConfig(level=level)
    logger = logging.getLogger(name)
    return logger


logger = get_logger(__name__)


def init_ray(args):
    if args.redis_address:
        ray.init(redis_address="{}:6379".format(args.redis_address))
        logger.info("Discovering nodes...")
        node_resources = read_node_names(num_nodes)
        [ping_node(node) for node in node_resources]
        logger.info("Discovered", len(node_resources), "resources:", node_resources)
    else:
        plasma_directory = "/mnt/hugepages" if args.huge_pages else None
        node_names = ["Node{}".format(i) for i in range(args.num_nodes)]
        resources = {node_name : 512 for node_name in node_names}
        ray.init(
            redirect_output=True,
            resources=resources,
            huge_pages=args.huge_pages is not None,
            plasma_directory=plasma_directory,
        )
    time.sleep(1)
    return node_names


def read_node_names(num_nodes):
    graph_bench_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(graph_bench_dir, 'conf/priv-hosts-all')) as f:
        lines = f.readlines()
    names = [l.strip() for l in lines][:num_nodes]
    if len(names) < num_nodes:
        raise IOError("File contains less than the requested number of nodes")
    return names


def ping_node(node_name):
    @ray.remote
    def ping():
        return socket.gethostname()
    name = ray.get(ping._remote(args=[], resources={node_name: 1}))
    if name != node_name:
        logger.info("Tried to ping %s but got %s", node_name, name)
    else:
        logger.info("Pinged %s", node_name)
    return name


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump', type=str, default="dump.json")
    parser.add_argument('--graph-fname', type=str, default=None)
    parser.add_argument('--huge-pages', type=str)
    parser.add_argument('--num-nodes', type=int, required=True)
    parser.add_argument('--num-subgraphs', type=int, required=True)
    parser.add_argument('--out-fname', type=str, default="test")
    parser.add_argument('--redis-address', type=str)
    parser.add_argument('--validate', action='store_true')
    args = parser.parse_args()
    return args


def flatten(lst):
    return [item for sublist in lst for item in sublist]


class Clock:
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.interval = time.time() - self.start

