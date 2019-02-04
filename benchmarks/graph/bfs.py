from __future__ import print_function
from collections import defaultdict, deque, namedtuple
import argparse
import logging
import time
import sys

import ray

import conf
import util

# Disambiguation: node refers to an instance running Ray, 
#                 vertex refers to a Graph vertex

class Vertex(object):
    def __init__(self, vertex, state, neighbours):
        self.vertex = vertex
        self.state = state
        self.neighbours = neighbours


class Subgraph(object):
    """
    Stores sub-graph as an adjacency-list.
    Additionally stores some metadata for each vertex.
    """
    
    def __init__(self):
        self.logger = util.get_logger(__name__)
        self.vertices = {}
        self.edges = defaultdict(set)

    def init_refs(self, idx, *subgraphs):
        self.my_idx = idx
        self.subgraphs = subgraphs
        self.num_subgraphs = len(self.subgraphs)

    def add_vertex(self, vertex, state=None):
        vertex_subgraph_idx = vertex % self.num_subgraphs
        assert vertex_subgraph_idx == self.my_idx
        self.vertices[vertex] = state

    def add_edge(self, src_vertex, dst_vertex, state=None):
        src_vertex_subgraph_idx = src_vertex % self.num_subgraphs
        dst_vertex_subgraph_idx = dst_vertex % self.num_subgraphs
        assert src_vertex_subgraph_idx == self.my_idx

        self.edges[src_vertex].add(dst_vertex)
        self.add_vertex(src_vertex, state)
        if dst_vertex_subgraph_idx == self.my_idx:
            self.add_vertex(dst_vertex, state)

    def num_vertices(self):
        return len(self.vertices)

    def num_edges(self):
        return sum(len(self.edges[v]) for v in self.edges)

    def foreach_vertex(self, func):
        """
        Apply vertex_state <- f(vertex, vertex_state) to each vertex
        """
        for vertex in self.vertices:
            state = self.vertices[vertex]
            self.vertices[vertex] = func(vertex, state)

    def apply(self, vertex, parent_state, func):
        """
        vertex_state <- f(vertex, vertex_state, parent_state)
        Return neighbors if state is updated, or an empty list if the state does not change.
        """
        assert vertex in self.vertices
        state = self.vertices[vertex]
        new_state = func(vertex, state, parent_state)
        self.vertices[vertex] = new_state 
        neighbours = self.edges[vertex] if state != new_state else []
        return Vertex(vertex, new_state, neighbours)

    def num_vertices(self):
        return len(self.vertices)


def init_subgraph(node_index):
    return ray.remote(num_cpus=0, resources={"Node{}".format(node_index): 1,})(Subgraph).remote()


class Graph(object):
    """
    Partitions data into sub-graphs
    """

    def __init__(self, num_subgraphs=1, num_nodes=1):
        self.num_subgraphs = num_subgraphs 
        self.subgraphs = [init_subgraph(i % num_nodes) for i in range(num_subgraphs)]
        for idx in range(self.num_subgraphs):
            ray.get([self.subgraphs[idx].init_refs.remote(idx, *self.subgraphs)])
        self.logger = util.get_logger(__name__)

    @property
    def num_vertices(self):
        total = 0
        subgraph_count_ids = [subgraph.num_vertices.remote() for subgraph in self.subgraphs]
        return sum(ray.get(subgraph_count_ids))

    @property
    def num_edges(self):
        total = 0
        subgraph_count_ids = [subgraph.num_edges.remote() for subgraph in self.subgraphs]
        return sum(ray.get(subgraph_count_ids))

    def add_vertex(self, vertex, state=None):
        """
        Adds vertex to graph
        """
        subgraph_idx = vertex % self.num_subgraphs
        obj_id = self.subgraphs[subgraph_idx].add_vertex.remote(vertex, state)
        ray.wait([obj_id])

    def add_edge(self, src_vertex, dst_vertex, state=None):
        """
        Adds edge and vertices to graph
        """
        src_vertex_subgraph_idx = src_vertex % self.num_subgraphs
        dst_vertex_subgraph_idx = dst_vertex % self.num_subgraphs
        obj_id = self.subgraphs[src_vertex_subgraph_idx].add_edge.remote(src_vertex, dst_vertex, state)
        ray.get([obj_id])
        if dst_vertex_subgraph_idx != src_vertex_subgraph_idx:
            obj_id = self.subgraphs[dst_vertex_subgraph_idx].add_vertex.remote(dst_vertex, state)
            ray.get([obj_id])

    def bfs_level_parallel(self, src_vertex, func):
        """
        Breadth-first search over graph. Applies func at each node.
        Parallelizes computation by level.
        """
        # have the src_vertex state updated to 0
        level = -1
        frontier = [Vertex(src_vertex, level, [src_vertex])]
        
        while frontier:
            neighbour_ids = []
            for v in frontier:
                ids = [self.subgraphs[w % self.num_subgraphs].apply.remote(w, level, func) for w in v.neighbours]
                neighbour_ids.extend(ids)
            level += 1
            frontier = ray.get(neighbour_ids)

    def bfs_fine_grained(self, src_vertex, func):
        """
        Breadth-first search over graph. Applies func at each node.
        """
        pass

    def foreach_vertex(self, func):
        """
        vertex.state <- f(vertex, state) for each vertex in the graph
        """
        ray.get([subgraph.foreach_vertex.remote(func) for subgraph in self.subgraphs])


def load_graph(file_path, num_subgraphs, num_nodes, init_state=sys.maxint):
    """
    Loads a graph from a CSV file containing edges
    Line Format: src, dest
    """
    g = Graph(num_subgraphs, num_nodes)
    if file_path:
        # TODO move data loading to nodes (or batch from driver)
        # ... or at least don't wait on every add_edge call
        a = time.time()
        lines_read = 0
        with open(file_path) as f:
            for line in f:
                l = line.strip()
                if lines_read % 1000000 == 0:
                    logger.debug("Read {} lines.".format(lines_read))
                lines_read += 1
                if not li.startswith("#"):
                    src_vertex, dst_vertex = (s.strip() for s in li.split(","))
                    g.add_edge(src_vetex, dst_vertex)
        b = time.time()
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


if __name__ == '__main__':
    args = util.parse_args()
    logger = util.get_logger(__name__)
    logger.info("Arguments: " + str(args))
    node_resources = ["Node{}".format(i) for i in range(args.num_nodes)]
    util.init_ray(args, node_resources)

    # Create graph
    g = load_graph(args.graph_fname, args.num_subgraphs, args.num_nodes)
    src_vertex = 1
 
    # Do BFS
    a = time.time()
    def f(v, state, parent_state):
        return state if state <= parent_state else parent_state + 1
    g.bfs_bsp_level_parallel(src_vertex, f)
    b = time.time()
    print("Time taken: ", b - a, "s")

    # Verify correctness
    def print_vertex(v, state):
        print(v, state)
        return state
    g.foreach_vertex(print_vertex)

    if args.dump:
        ray.global_state.chrome_tracing_dump(filename=args.dump)

