from collections import defaultdict, deque, namedtuple
import copy
import logging
import time
import sys

import ray

import util


class Graph(object):
    """
    Partitions data into sub-graphs
    """

    def __init__(self, num_subgraphs=1, num_nodes=1):
        self.num_subgraphs = num_subgraphs 
        self.subgraphs = [init_subgraph(i % num_nodes) for i in range(num_subgraphs)]
        for idx in range(self.num_subgraphs):
            ray.get(self.subgraphs[idx].init_refs.remote(idx, *self.subgraphs))
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
        Adds vertex to the graph.
        Blocks until completion.
        """
        subgraph_idx = vertex % self.num_subgraphs
        ray.get(self.subgraphs[subgraph_idx].add_vertex.remote(vertex, state))

    def add_edge(self, src_vertex, dst_vertex, state=None):
        """
        Adds edge and vertices to the graph.
        Blocks until completion.
        """
        src_subgraph_idx = src_vertex % self.num_subgraphs
        dst_subgraph_idx = dst_vertex % self.num_subgraphs
        ids = []
        ids.append(self.subgraphs[src_subgraph_idx].add_edge.remote(src_vertex, dst_vertex, state))
        if dst_subgraph_idx != src_subgraph_idx:
            ids.append(self.subgraphs[dst_subgraph_idx].add_vertex.remote(dst_vertex, state))
        ray.get(ids)

    def get_vertex_state(self, vertex):
        subgraph_idx = vertex % self.num_subgraphs
        return ray.get(self.subgraphs[subgraph_idx].get_vertex_state.remote(vertex))

    def apply(self, f, vertex, graph_context=None):
        """
        Returns a reference to the vertex with its neighbors if state is updated,
        or with an empty list if the state does not change.
        vertex_state <- f(vertex, vertex_state, graph_context)
        """
        return self.subgraphs[vertex % self.num_subgraphs].apply.remote(f, vertex, graph_context)

    def foreach_vertex(self, f, graph_context=None):
        """
        Applies function to each vertex.
        vertex_state <- f(vertex, vertex_state, graph_context) for each vertex in the graph
        Blocks until completion.
        """
        ray.get([subgraph.foreach_vertex.remote(f, graph_context) for subgraph in self.subgraphs])

    def recursive_foreach_vertex(self, f, src_vertex, graph_context=None):
        """
        Recursively applies function to each vertex, starting at vertex.
        f: vertex_state <- f(vertex, vertex_state)
        Blocks until completion.
        """
        src_subgraph_idx = src_vertex % self.num_subgraphs
        ids = [self.subgraphs[src_subgraph_idx].recursive_foreach_vertex.remote(f, src_vertex, graph_context)]
        while ids:
            ids += ray.get(ids.pop(-1))

    def load_from_file(self, file_path, delim='\t'):
        """
        Loads edges from file. 
        Blocks until completion.
        """
        ray.get([self.subgraphs[i].load_from_file.remote(file_path, delim) for i in range(self.num_subgraphs)])


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

    def num_vertices(self):
        return len(self.vertices)

    def num_edges(self):
        return sum(len(self.edges[v]) for v in self.edges)

    def add_vertex(self, vertex, state=None):
        vertex_subgraph_idx = vertex % self.num_subgraphs
        assert vertex_subgraph_idx == self.my_idx
        self.vertices[vertex] = state

    def add_edge(self, src_vertex, dst_vertex, state=None):
        src_subgraph_idx = src_vertex % self.num_subgraphs
        dst_subgraph_idx = dst_vertex % self.num_subgraphs
        assert src_subgraph_idx == self.my_idx

        self.edges[src_vertex].add(dst_vertex)
        self.add_vertex(src_vertex, state)
        if dst_subgraph_idx == self.my_idx:
            self.add_vertex(dst_vertex, state)

    def get_vertex_state(self, vertex):
        assert vertex in self.vertices
        return self.vertices[vertex]

    def apply(self, f, vertex, graph_context):
        """
        vertex_state <- f(vertex, vertex_state, graph_context)
        Returns vertex with its neighbors if state is updated,
        or with an empty list if the state does not change.
        """
        assert vertex in self.vertices
        state = self.vertices[vertex]
        new_state = f(vertex, state, graph_context)
        self.vertices[vertex] = new_state 
        neighbours = self.edges[vertex] if state != new_state else []
        return Vertex(vertex, new_state, neighbours)

    def foreach_vertex(self, f, graph_context):
        """
        Applies function to each vertex in the sub-graph.
        """
        for vertex in self.vertices:
            state = self.vertices[vertex]
            self.vertices[vertex] = f(vertex, state, graph_context)

    def recursive_foreach_vertex(self, f, vertex, graph_context):
        """
        Applies function to vertex and its neighbours recursively.
        Forwards call to other sub-graphs if necessary.
        vertex_state <- f(vertex, vertex_state, graph_context)
        """
        assert vertex in self.vertices
        self.logger.debug("[%s] recv v%s", self.my_idx, vertex)

        state = self.vertices[vertex]
        new_state = f(vertex, state, graph_context)
        self.vertices[vertex] = new_state

        if state == new_state:
            return []
        else:
            # Recurse on neighbours
            ids = []
            for neighbour in self.edges[vertex]:
                # TODO batch calls
                neighbour_subgraph_idx = neighbour % self.num_subgraphs
                if neighbour_subgraph_idx == self.my_idx:
                    # We need to make a copy of the context for each neighbour
                    # so that it doesn't remain mutated after the call returns.
                    # However, maybe these semantics are wrong for some use-cases (?)
                    # TODO Probably not though but make sure
                    context_copy = copy.copy(graph_context)
                    ids += self.recursive_foreach_vertex(f, neighbour, context_copy)
                else:
                    subgraph = self.subgraphs[neighbour_subgraph_idx]
                    # We don't need to copy the context since it is copied by default over IPC.
                    ids.append(subgraph.recursive_foreach_vertex.remote(f, neighbour, graph_context))
            return ids

    def load_from_file(self, file_path, delim='\t'):
        """
        Loads only the vertices/edges that belong 
        to this subgraph from the file.
        """
        lines_read = 0
        with open(file_path) as infile:
            for line in infile:
                li = line.strip()
                if lines_read % 1000000 == 0:
                    self.logger.debug("Subgraph{} read {} lines.".format(self.my_idx, lines_read))
                lines_read += 1

                src_vertex, dst_vertex = (int(s) for s in li.split(delim))

                src_subgraph_idx = src_vertex % self.num_subgraphs
                dst_subgraph_idx = dst_vertex % self.num_subgraphs
                if src_subgraph_idx == self.my_idx:
                    self.add_edge(src_vertex, dst_vertex)
                if dst_subgraph_idx == self.my_idx:
                    self.add_vertex(dst_vertex)


def init_subgraph(node_index):
    return ray.remote(num_cpus=0, resources={"Node{}".format(node_index): 1,})(Subgraph).remote()


class Vertex(object):

    def __init__(self, vertex, state, neighbours):
        self.vertex = vertex
        self.state = state
        self.neighbours = neighbours


class Edge(object):

    def __init__(self, src_vertex, dst_vertex):
        self.src_vertex = src_vertex
        self.dst_vertex = dst_vertex

