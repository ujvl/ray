from __future__ import print_function
from collections import defaultdict, deque, namedtuple
import argparse
import logging
import time
import sys

import numpy as np
import ray

from graph import Graph, Vertex
from util import Clock
import conf
import util

# Disambiguation: node refers to an instance running Ray,
#                 vertex refers to a Graph vertex


@ray.remote
def warmup_objectstore():
    x = np.ones(10 ** 8)
    for _ in range(100):
        ray.put(x)


def load_graph(file_path, num_subgraphs, node_names, init_state=float("inf")):
    """
    Loads a graph
    """
    g = Graph(num_subgraphs, node_names)
    if file_path:
        with Clock() as c:
            g.load_from_file(file_path)
            g.foreach_vertex(lambda v, state, context: init_state)
        logger.info("Graph loaded in {}s.".format(c.interval))
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
    logger.info("|V|=%s, |E|=%s", g.num_vertices, g.num_edges)
    return g


def bfs_level_parallel(graph, src_vertex):
    """
    Breadth-first search over graph.
    Parallelizes computation by level.
    """
    # have the src_vertex state updated to 0
    level = -1
    frontier = [Vertex(None, level, [src_vertex])]

    while frontier:
        next_frontier_ids = []
        for v in frontier:
            ids = [graph.apply(increment_level, w, graph_context=level) for w in v.neighbours]
            next_frontier_ids += ids
        level += 1
        frontier = ray.get(next_frontier_ids)


def bfs_batch_level_parallel(graph, src_vertex):
    """
    Batched BFS over graph.
    """
    # have the src_vertex state updated to 0
    level = -1

    #frontier = [[Vertex(None, level, [src_vertex])]]
    # while frontier:
    #     batches = [[] for _ in range(graph.num_subgraphs)]
    #     for result_batch in frontier:
    #         for v in result_batch:
    #             for w in v.neighbours:
    #                 batches[graph.subgraph_of(w)].append(w)
    #     next_frontier_ids = [graph.batch_apply(increment_level, b, graph_context=level) for b in batches if len(b)]

    #     level += 1
    #     frontier = ray.get(next_frontier_ids)

    frontier = [[src_vertex]]
    while frontier:
        batches = [[] for _ in range(graph.num_subgraphs)]
        for result in frontier:
            for v in frontier:
                batches[graph.subgraph_of(v)].append(v)
        next_frontier_ids = [graph.batch_apply(increment_level, b, graph_context=level) for b in batches if len(b)]

        level += 1
        frontier = ray.get(next_frontier_ids)


def increment_level(v, state, level):
    """
    Increment level if it leads to a new smaller level.
    new_state, context <- f(vertex, state, context)
    """
    level = min(state, level + 1)
    logger.debug("v%s: %s -> %s", v, state, level)
    return level


def print_vertex(v, state, context):
    logger.info("v%s state = %s", v, state)
    return state


if __name__ == '__main__':
    args = util.parse_args()
    logger = util.get_logger(__name__)
    logger.info("Arguments: " + str(args))
    node_names = util.init_ray(args)
    logger.info("Warming up...")
    with Clock() as c:
        pass
        # ray.get(warmup_objectstore.remote())
    logger.info("Time taken: %s ms", c.interval * 1000)

    src_vertex = 1

    try:
        g = load_graph(args.graph_fname, args.num_subgraphs, node_names)

        # Do coordinated BFS
        logger.info("Running coordinated BFS...")
        time.sleep(1)
        with Clock() as c:
            bfs_batch_level_parallel(g, src_vertex)
        coord_state = g.state()
        logger.info("Time taken: %s ms", c.interval * 1000)
        logger.info("Calls: %s", g.calls())

        logger.info("Clearing...")
        time.sleep(1)
        g.clear_stats()
        g.foreach_vertex(lambda v, state, ctxt: float("inf"))

        # Do uncoordinated BFS
        logger.info("Running recursive BFS...")
        time.sleep(1)
        with Clock() as c:
            g.recursive_foreach_vertex(increment_level, src_vertex, graph_context=-1, batch=True)
        logger.info("Time taken: %s ms", c.interval * 1000)
        logger.info("Calls: %s", g.calls())

        # Validate against each other
        if args.validate:
            time.sleep(1)
            def validate(v, state, context):
                coordinated_state = coord_state[v]
                logger.debug("v%s: coord-state = %s, uncoord-state = %s", v, coordinated_state, state)
                assert state == coordinated_state
                return state
            g.foreach_vertex(validate)

    except KeyboardInterrupt:
        pass

    logger.info("Dumping state...")
    ray.global_state.chrome_tracing_dump(filename=args.dump)

