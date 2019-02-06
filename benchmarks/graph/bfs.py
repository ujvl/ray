from __future__ import print_function
from collections import defaultdict, deque, namedtuple
import argparse
import logging
import time
import sys

import ray

from graph import Graph, Vertex
import conf
import util

# Disambiguation: node refers to an instance running Ray, 
#                 vertex refers to a Graph vertex


def load_graph(file_path, num_subgraphs, num_nodes, init_state=sys.maxint):
    """
    Loads a graph
    """
    g = Graph(num_subgraphs, num_nodes)
    if file_path:
        a = time.time()
        g.load_from_file(file_path)
        b = time.time()
        g.foreach_vertex(lambda v, state, context: init_state)
        logger.info("Graph loaded in {}s.".format(b - a))
    else:
        # Toy graph
        g.add_edge(1, 2, init_state)
        g.add_edge(1, 3, init_state)
        g.add_edge(1, 4, init_state)
        g.add_edge(2, 3, init_state)
        g.add_edge(2, 5, init_state)
        g.add_edge(4, 1, init_state)
        g.add_edge(5, 3, init_state)
        g.add_edge(5, 6, init_state)
    logger.info("%s=%s, %s=%s", "|V|", g.num_vertices, "|E|", g.num_edges)
    return g


def bfs_level_parallel(graph, src_vertex):
    """
    Breadth-first search over graph.
    Parallelizes computation by level.
    """
    # have the src_vertex state updated to 0
    context = BFSContext(-1)
    frontier = [Vertex(src_vertex, context.level, [src_vertex])]
    
    while frontier:
        neighbour_ids = []
        # Bind parent state param
        for v in frontier:
            ids = [graph.apply(increment_level, w, context) for w in v.neighbours]
            neighbour_ids.extend(ids)
        context.level += 1
        frontier = ray.get(neighbour_ids)


class BFSContext:

    def __init__(self, level):
        self.level = level


def increment_level(v, state, context):
    """
    Increment level if it leads to a new smaller level.
    """
    logger.debug("v%s, state = %s, parent level = %s", v, state, context.level)
    time.sleep(0.01)
    context.level = min(state, context.level + 1)
    return context.level


def print_vertex(v, state, context):
    logger.info("v%s state = %s", v, state)
    return state


if __name__ == '__main__':
    args = util.parse_args()
    logger = util.get_logger(__name__)
    logger.info("Arguments: " + str(args))
    node_resources = ["Node{}".format(i) for i in range(args.num_nodes)]
    util.init_ray(args, node_resources)

    # Create graphs
    g = load_graph(args.graph_fname, args.num_subgraphs, args.num_nodes)
    g2 = load_graph(args.graph_fname, args.num_subgraphs, args.num_nodes)
    src_vertex = 1
 
    # Do coordinated BFS
    logger.info("Running BFS...")
    a = time.time()
    bfs_level_parallel(g, src_vertex)
    b = time.time()
    logger.info("Time taken: %s ms", (b - a) * 1000)

    # Do uncoordinated BFS
    logger.info("Running recursive BFS...")
    a = time.time()
    g2.recursive_foreach_vertex(increment_level, src_vertex, BFSContext(-1))
    b = time.time()
    logger.info("Time taken: %s ms", (b - a) * 1000)

    # Verify correctness
    def verify(v, state, context):
        uncoordinated_state = g2.get_vertex_state(v)
        assert uncoordinated_state == state
        logger.debug("%sv: coord-state = %s, uncoord-state = %s", v, state, uncoordinated_state)
        return state
    g.foreach_vertex(verify)

    if args.dump:
        ray.global_state.chrome_tracing_dump(filename=args.dump)

