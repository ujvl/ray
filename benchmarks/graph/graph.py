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

    def __init__(self, num_subgraphs, node_names):
        self.num_subgraphs = num_subgraphs
        self.logger = util.get_logger(__name__)
        # Enable no-lambda-parameter optimization
        self.registered_function = None
        # Distribute subgraphs over nodes evenly
        num_nodes = len(node_names)
        self.subgraphs = [init_subgraph(node_names[i % num_nodes]) for i in range(num_subgraphs)]
        for i in range(self.num_subgraphs):
            ray.get(self.subgraphs[i].init_refs.remote(i, *self.subgraphs))

    def register_function(self, func):
        """
        Broadcasts a function for better perf (if it hasn't already been broadcasted).
        """
        if self.registered_function != func:
            ray.get([sub.register_function.remote(func) for sub in self.subgraphs])
            self.registered_function = func

    def clear_stats(self):
        ray.get([sub.clear_stats.remote() for sub in self.subgraphs])

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

    def state(self):
        sub_states = ray.get([sub.state.remote() for sub in self.subgraphs])
        state = {}
        for sub_state in sub_states:
            for vertex in sub_state:
                state[vertex] = sub_state[vertex]
        return state

    def apply(self, f, vertex, graph_context=None):
        """
        Applies function to vertex.
        Returns object ID to the vertex with its neighbors if state is updated,
        or with an empty list if the state does not change.
        vertex_state <- f(vertex, vertex_state, graph_context)
        """
        self.register_function(f)
        sub = self.subgraphs[self.subgraph_of(vertex)]
        return sub.apply.remote(vertex, graph_context)

    def batch_apply(self, f, vertices, graph_context=None):
        """
        Applies function to batch of vertices.
        (assuming they all belong to the same subgraph)
        """
        assert len(vertices)
        self.register_function(f)
        sub = self.subgraphs[self.subgraph_of(vertices[0])]
        return sub.batch_apply.remote(vertices, graph_context)

    def foreach_vertex(self, f, graph_context=None):
        """
        Applies function to each vertex.
        vertex_state <- f(vertex, vertex_state, graph_context) for each vertex in the graph
        Blocks until completion.
        """
        self.register_function(f)
        ray.get([sub.foreach_vertex.remote(graph_context) for sub in self.subgraphs])

    def recursive_foreach_vertex(self, f, src_vertex, graph_context=None, batch=True):
        """
        Recursively applies function to each vertex, starting at vertex.
        f: vertex_state <- f(vertex, vertex_state)
        Blocks until completion.
        """
        self.register_function(f)
        subgraph_idx = self.subgraph_of(src_vertex)
        sub = self.subgraphs[subgraph_idx]
        if batch:
            arg = Batch([src_vertex], graph_context)
            ids = [sub.batch_recursive_foreach_vertex.remote(arg)]
        else:
            ids = [sub.recursive_foreach_vertex.remote(src_vertex, graph_context)]

        while ids:
            ids = util.flatten(ray.get(ids))

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
        self.state = {} # vertex -> state
        self.edges = defaultdict(set)
        self.clear_stats()

    def init_refs(self, idx, *subgraphs):
        self.my_idx = idx
        self.subgraphs = subgraphs
        self.num_subgraphs = len(self.subgraphs)

    def register_function(self, func):
        self.func = func

    def clear_stats(self):
        self.calls = 0

    def num_vertices(self):
        return len(self.state)

    def num_edges(self):
        return sum(len(self.edges[v]) for v in self.edges)

    def add_vertex(self, vertex, state=None):
        vertex_subgraph_idx = vertex % self.num_subgraphs
        assert vertex_subgraph_idx == self.my_idx
        self.state[vertex] = state

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
        assert vertex in self.state
        return self.state[vertex]

    def state(self):
        return self.state

    def apply(self, vertex, graph_context):
        """
        Applies function to vertex.
        Returns vertex with its neighbors if state is updated,
        or with an empty list if the state does not change.
        vertex_state <- f(vertex, vertex_state, graph_context)
        """
        assert vertex in self.state
        state = self.state[vertex]
        new_state = self.func(vertex, state, graph_context)
        self.state[vertex] = new_state
        neighbours = self.edges[vertex] if state != new_state else []
        self.calls += 1
        #return Vertex(vertex, new_state, neighbours)
        return neighbours

    def batch_apply(self, vertices, graph_context):
        # TODO assumes entire batch of vertices belong to this subgraph
        #return [self.apply(vertex, f, graph_context) for vertex in vertices]
        result = set()
        for vertex in vertices:
            result.update(self.apply(vertex, graph_context))
        return result

    def foreach_vertex(self, graph_context):
        """
        Applies function to each vertex in the sub-graph.
        vertex_state <- f(vertex, vertex_state, graph_context)
        """
        for vertex in self.state:
            state = self.state[vertex]
            self.state[vertex] = self.func(vertex, state, graph_context)

    def recursive_foreach_vertex(self, vertex, graph_context):
        """
        Applies function to vertex and its neighbours recursively.
        Forwards call to other sub-graphs if necessary.
        vertex_state <- f(vertex, vertex_state, graph_context)
        """
        assert vertex in self.state
        self.logger.debug("[%s] recv v%s", self.my_idx, vertex)

        state = self.state[vertex]
        new_state = self.func(vertex, state, graph_context)
        self.state[vertex] = new_state

        ids = []
        self.calls += 1
        # Recurse on neighbours
        if state != new_state:
            for neighbour in self.edges[vertex]:
                neighbour_subgraph_idx = neighbour % self.num_subgraphs
                subgraph = self.subgraphs[neighbour_subgraph_idx]
                ids.append(subgraph.recursive_foreach_vertex.remote(neighbour, new_state))
        return ids

    def batch_recursive_foreach_vertex(self, batch_arg):
        """
        Applies function to batch of vertices and their neighbours recursively.
        Forwards call to other sub-graphs if necessary.
        vertex_state <- f(vertex, vertex_state)
        """
        batch_state = None
        batches = [set() for _ in range(self.num_subgraphs)]
        for vertex in batch_arg.vertices:
            ctxt = batch_arg.graph_context
            self.calls += 1

            state = self.state[vertex]
            new_state = self.func(vertex, state, ctxt)
            self.state[vertex] = new_state

            if state != new_state:
                if not batch_state:
                    batch_state = new_state
                for neighbour in self.edges[vertex]:
                    batches[neighbour % self.num_subgraphs].add(vertex)

        ids = []
        if batch_state is not None:
            batch_args = []
            for batch in batches:
                batch_args.append(Batch(batch, batch_state))

            for sub_idx in range(self.num_subgraphs):
                if len(batches[sub_idx]):
                    ids.append(self.subgraphs[sub_idx].batch_recursive_foreach_vertex.remote(
                        batch_args[sub_idx],
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


def init_subgraph(node_name):
    return ray.remote(num_cpus=0, resources={node_name: 1,})(Subgraph).remote()


class Batch(ray.worker.SimpleType):

    def __init__(self, vertices, graph_context):
        self.vertices = vertices
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

