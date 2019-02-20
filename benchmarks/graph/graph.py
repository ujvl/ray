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

    def register_function(self, func):
        """
        Broadcasts a function for better perf
        """
        ray.get([sub.register_function.remote(func) for sub in self.subgraphs])

    @property
    def num_vertices(self):
        return sum(ray.get([sub.num_vertices.remote() for sub in self.subgraphs]))

    @property
    def num_edges(self):
        return sum(ray.get([sub.num_edges.remote() for sub in self.subgraphs]))

    def subgraph_of(self, vertex):
        # TODO move this into some policy/config
        return vertex % self.num_subgraphs

    def add_vertex(self, vertex, state=None):
        """
        Adds vertex to the graph.
        Blocks until completion.
        """
        subgraph_idx = self.subgraph_of(vertex)
        ray.get(self.subgraphs[subgraph_idx].add_vertex.remote(vertex, state))

    def add_edge(self, src_vertex, dst_vertex, state=None):
        """
        Adds edge and vertices to the graph.
        Blocks until completion.
        """
        src_subgraph_idx = self.subgraph_of(src_vertex)
        dst_subgraph_idx = self.subgraph_of(dst_vertex)
        ids = []
        ids.append(self.subgraphs[src_subgraph_idx].add_edge.remote(src_vertex, dst_vertex, state))
        if dst_subgraph_idx != src_subgraph_idx:
            ids.append(self.subgraphs[dst_subgraph_idx].add_vertex.remote(dst_vertex, state))
        ray.get(ids)

    def get_vertex_state(self, vertex):
        subgraph_idx = self.subgraph_of(vertex)
        return ray.get(self.subgraphs[subgraph_idx].get_vertex_state.remote(vertex))

    def apply(self, vertex, f=None, graph_context=None):
        """
        Returns object ID to the vertex with its neighbors if state is updated,
        or with an empty list if the state does not change.
        vertex_state <- f(vertex, vertex_state, graph_context)
        """
        sub = self.subgraphs[self.subgraph_of(vertex)]
        if f:
            return sub.apply.remote(vertex, f, graph_context)
        else:
            return sub._apply.remote(vertex, graph_context)

    def batch_apply(self, vertices, f=None, graph_context=None):
        """
        Returns object IDs to the vertices whose states are updated.
        """
        assert len(vertices)
        # TODO Assumes entire batch belongs to the same subgraph
        sub = self.subgraphs[self.subgraph_of(vertices[0])]
        if f:
            return sub.batch_apply.remote(vertices, f, graph_context)
        else:
            return sub._batch_apply.remote(vertices, graph_context)

    def foreach_vertex(self, f=None, graph_context=None):
        """
        Applies function to each vertex.
        vertex_state <- f(vertex, vertex_state, graph_context) for each vertex in the graph
        Blocks until completion.
        """
        if f:
            ray.get([sub.foreach_vertex.remote(f, graph_context) for sub in self.subgraphs])
        else:
            ray.get([sub._foreach_vertex.remote(graph_context) for sub in self.subgraphs])

    def recursive_foreach_vertex(self, src_vertex, f=None, graph_context=None, batch=True):
        """
        Recursively applies function to each vertex, starting at vertex.
        f: vertex_state <- f(vertex, vertex_state)
        Blocks until completion.
        """
        subgraph_idx = self.subgraph_of(src_vertex)
        sub = self.subgraphs[subgraph_idx]
        if batch:
            arg = BatchArg(src_vertex, graph_context)
            if f:
                ids = [sub.batch_recursive_foreach_vertex.remote(f, [arg])]
            else:
                ids = [sub._batch_recursive_foreach_vertex.remote([arg])]
        else:
            if f:
                ids = [sub.recursive_foreach_vertex.remote(src_vertex, f, graph_context)]
            else:
                ids = [sub._recursive_foreach_vertex.remote(src_vertex, graph_context)]

        while ids:
            ids += ray.get(ids.pop(-1))

    def load_from_file(self, file_path, delim='\t'):
        """
        Loads edges from file.
        Blocks until completion.
        """
        ray.get([sub.load_from_file.remote(file_path, delim) for sub in self.subgraphs])

    def calls(self):
        return sum(ray.get([sub.num_calls.remote() for sub in self.subgraphs]))


class Subgraph(object):
    """
    Stores sub-graph as an adjacency-list.
    Additionally stores some metadata for each vertex.
    """

    def __init__(self):
        self.logger = util.get_logger(__name__)
        self.vertices = {}
        self.edges = defaultdict(set)

        self.calls = 0

    def init_refs(self, idx, *subgraphs):
        self.my_idx = idx
        self.subgraphs = subgraphs
        self.num_subgraphs = len(self.subgraphs)

    def register_function(self, func):
        self.func = func

    def num_vertices(self):
        return len(self.vertices)

    def num_edges(self):
        return sum(len(self.edges[v]) for v in self.edges)

    def add_vertex(self, vertex, state=None):
        vertex_subgraph_idx = vertex % self.num_subgraphs
        assert vertex_subgraph_idx == self.my_idx
        self.vertices[vertex] = state

    def num_calls(self):
        return self.calls

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

    def apply(self, vertex, f, graph_context):
        """
        Applies function on vertex.
        Returns vertex with its neighbors if state is updated,
        or with an empty list if the state does not change.
        vertex_state <- f(vertex, vertex_state, graph_context)
        """
        assert vertex in self.vertices
        state = self.vertices[vertex]
        new_state = f(vertex, state, graph_context)
        self.vertices[vertex] = new_state
        neighbours = self.edges[vertex] if state != new_state else []
        self.calls += 1
        return Vertex(vertex, new_state, neighbours)

    def batch_apply(self, vertices, f, graph_context):
        # TODO assumes entire batch of vertices belong to this subgraph
        return [self.apply(vertex, f, copy.copy(graph_context)) for vertex in vertices]

    def foreach_vertex(self, f, graph_context):
        """
        Applies function to each vertex in the sub-graph.
        vertex_state <- f(vertex, vertex_state, graph_context)
        """
        for vertex in self.vertices:
            state = self.vertices[vertex]
            self.vertices[vertex] = f(vertex, state, graph_context)

    def recursive_foreach_vertex(self, vertex, f, graph_context):
        """
        Applies function to vertex and its neighbours recursively.
        Forwards call to other sub-graphs if necessary.
        vertex_state, graph_context <- f(vertex, vertex_state, graph_context)
        """
        assert vertex in self.vertices
        self.logger.debug("[%s] recv v%s", self.my_idx, vertex)

        state = self.vertices[vertex]
        new_state, graph_context = f(vertex, state, graph_context)
        self.vertices[vertex] = new_state

        ids = []
        self.calls += 1
        # Recurse on neighbours
        if state != new_state:
            for neighbour in self.edges[vertex]:
                neighbour_subgraph_idx = neighbour % self.num_subgraphs
                subgraph = self.subgraphs[neighbour_subgraph_idx]
                ids.append(subgraph.recursive_foreach_vertex.remote(
                    f,
                    neighbour,
                    graph_context,
                ))
        return ids

    def batch_recursive_foreach_vertex(self, f, arg_batch):
        """
        Applies function to batch of vertices and their neighbours recursively.
        Forwards call to other sub-graphs if necessary.
        vertex_state, graph_context <- f(vertex, vertex_state, graph_context)
        """
        ids = []
        batches = [[] for _ in range(self.num_subgraphs)]
        for arg in arg_batch:
            vertex, ctxt = arg.vertex, arg.graph_context
            self.calls += 1

            state = self.vertices[vertex]
            new_state, ctxt = f(vertex, state, ctxt)
            self.vertices[vertex] = new_state

            if state != new_state:
                for neighbour in self.edges[vertex]:
                    arg = BatchArg(neighbour, ctxt)
                    batches[neighbour % self.num_subgraphs].append(arg)

        for batch in range(self.num_subgraphs):
            if len(batches[batch]):
                ids.append(self.subgraphs[batch].batch_recursive_foreach_vertex.remote(
                    f,
                    batches[batch],
                ))
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
        self.logger.info(
                "[%s] sub-graph size: |V|=%s,|E|=%s",
                self.my_idx, self.num_vertices(), self.num_edges()
        )

    # --------------------------
    # Optimized member functions
    # --------------------------

    def _apply(self, vertex, graph_context):
        return self.apply(vertex, self.func, graph_context)

    def _batch_apply(self, vertices, graph_context):
        return self.batch_apply(vertices, self.func, graph_context)

    def _foreach_vertex(self, graph_context):
        return self.foreach_vertex(vertices, self.func, graph_context)

    def _recursive_foreach_vertex(self, vertex, graph_context):
        assert vertex in self.vertices
        self.logger.debug("[%s] recv v%s", self.my_idx, vertex)

        state = self.vertices[vertex]
        new_state, graph_context = self.func(vertex, state, graph_context)
        self.vertices[vertex] = new_state

        ids = []
        self.calls += 1
        # Recurse on neighbours
        if state != new_state:
            for neighbour in self.edges[vertex]:
                neighbour_subgraph_idx = neighbour % self.num_subgraphs
                subgraph = self.subgraphs[neighbour_subgraph_idx]
                ids.append(subgraph._recursive_foreach_vertex.remote(neighbour, graph_context))
        return ids

    def _batch_recursive_foreach_vertex(self, arg_batch):
        ids = []
        batches = [[] for _ in range(self.num_subgraphs)]
        for arg in arg_batch:
            vertex, ctxt = arg.vertex, arg.graph_context
            self.calls += 1

            state = self.vertices[vertex]
            new_state, ctxt = self.func(vertex, state, ctxt)
            self.vertices[vertex] = new_state

            if state != new_state:
                for neighbour in self.edges[vertex]:
                    arg = BatchArg(neighbour, ctxt)
                    batches[neighbour % self.num_subgraphs].append(arg)

        for batch_idx in range(self.num_subgraphs):
            batch = batches[batch_idx]
            if len(batch):
                ids.append(self.subgraphs[batch_idx]._batch_recursive_foreach_vertex.remote(batch))
        return ids


def init_subgraph(node_index):
    return ray.remote(num_cpus=0, resources={"Node{}".format(node_index): 1,})(Subgraph).remote()


class BatchArg(object):

    def __init__(self, vertex, graph_context):
        self.vertex = vertex
        self.graph_context = graph_context


class Vertex(object):

    def __init__(self, vertex, state, neighbours):
        self.vertex = vertex
        self.state = state
        self.neighbours = neighbours


class Edge(object):

    def __init__(self, src_vertex, dst_vertex):
        self.src_vertex = src_vertex
        self.dst_vertex = dst_vertex

