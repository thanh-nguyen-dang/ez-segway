import sys

import networkx as nx
import fnss

from misc import logger, constants
from sphere import Sphere

log = logger.getLogger("topology", constants.LOG_LEVEL)

class Topology:
    def __init__(self, graph):
        self.graph = graph

    @staticmethod
    def read_adjacency(filename):
        """Reads adjacency matrix from a file.

        Returns:
            list[list of bool]
        """
        def IntLineReader(filename):
            with open(filename) as f:
                for line in f:
                    yield [bool(int(x)) for x in line.strip().split(' ')]
        adjacency = list(IntLineReader(filename))
        graph = nx.Graph()
        src = 0
        for row in adjacency:
            for dst in range(src+1, len(row)):
                if row[dst] > 0:
                    graph.add_edge(src, dst, weight=constants.DEFAULT_CAP)
            src += 1
        return Topology(graph)

    @staticmethod
    def read_weighted_adjacency(filename):
        def FloatLineReader(filename):
            with open(filename) as f:
                for line in f:
                    yield [float(x) for x in line.strip().split(' ')]
        adjacency = list(FloatLineReader(filename))
        graph = nx.Graph()
        src = 0
        for row in adjacency:
            for dst in range(src+1, len(row)):
                if row[dst] > 0:
                    graph.add_edge(src, dst, weight=constants.MAX_CAP, delay=float(row[dst]))
            src += 1
        return Topology(graph)

    @staticmethod
    def read_rocketfuel_latencies(latency_file, weight_file):
        debug = log.debug
        debug(latency_file)
        debug(weight_file)
        topology = fnss.parse_rocketfuel_isp_latency(latency_file, weight_file)
        return Topology(nx.Graph(topology.to_directed()))

    @staticmethod
    def read_zoo_topology_gml(zoo_file):
        g = nx.read_gml(zoo_file, label="id")
        log.info(g.nodes(data=True))
        for n, items in g.nodes_iter(data=True):
            log.info("node {0}: {1}".format(str(n), str(items)))
        return Topology(nx.Graph(g.to_directed()))

    def compute_distances_zoo_topology(self):
        latencies = {}
        max_weight = 0
        min_weight = sys.maxint
        for n, nbrs in self.graph.adjacency_iter():
            for nbr, eattr in nbrs.items():
                log.info("edge {0}-{1}: {2}".format(n, nbr, str(eattr)))
                n, n_items = self.graph.nodes(data=True)[n]
                nbr, nbr_items = self.graph.nodes(data=True)[nbr]
                latencies[(n, nbr)] = float(Sphere.distance(n_items['Latitude'], n_items['Longitude'],
                                                            nbr_items['Latitude'], nbr_items['Longitude']) * 1000)/200000
                log.info("latency: {0}".format(latencies[(n, nbr)]))
        return latencies


    def get_highest_degree_node(self):
        highest_degree_id = 0
        highest_degree = 0
        for n in self.graph.nodes():
            if self.graph.degree(n) > highest_degree:
                highest_degree = self.graph.degree(n)
                highest_degree_id = n
        return highest_degree_id, highest_degree

    def deploy_controller(self, sw, sw_to_ctrl_delays):
        sw_to_ctrl_delays[sw] = 0
        shortest_paths = nx.shortest_path(self.graph, target=sw, weight='delay')
        shortest_path_lengths = nx.shortest_path_length(self.graph, target=sw, weight='delay')
        log.info(shortest_paths)
        for n in self.graph.nodes():
            if n == sw:
                continue
            if n in shortest_path_lengths.keys():
                sw_to_ctrl_delays[n] = shortest_path_lengths[n]
            else:
                sw_to_ctrl_delays[n] = 1
        log.debug("sw to ctrl delays: %s" % str(sw_to_ctrl_delays))

    def from_weight_to_capacity(self, min_weight, max_weight):
        capacities = {}
        for n, nbrs in self.graph.adjacency_iter():
            for nbr, eattr in nbrs.items():
                capacities[(n, nbr)] = constants.MAX_CAP
                # weight = eattr['weight']
                # if max_weight > 3 * weight:
                #     capacities[(n, nbr)] = constants.MAX_CAP
                # elif 2 * max_weight > 3 * weight:
                #     capacities[(n, nbr)] = constants.MAX_CAP/10
                # else:
                #     capacities[(n, nbr)] = constants.MAX_CAP/100
        return capacities

    def export_rocketfuel_capacity_and_latency(self):
        latencies = {}
        max_weight = 0
        min_weight = sys.maxint
        for n, nbrs in self.graph.adjacency_iter():
            for nbr, eattr in nbrs.items():
                latencies[(n, nbr)] = eattr['delay']
                # weight = eattr['weight']
                # has_weight = True
                # if weight < min_weight:
                #     min_weight = weight
                # if weight > max_weight:
                #     max_weight = weight
        capacities = self.from_weight_to_capacity(min_weight, max_weight)
        return capacities, latencies

    def export_capacity_and_latency(self):
        latencies = {}
        capacities = {}
        max_weight = 0
        min_weight = sys.maxint
        for n, nbrs in self.graph.adjacency_iter():
            for nbr, eattr in nbrs.items():
                latencies[(n, nbr)] = eattr['delay']
                capacities[(n, nbr)] = eattr['weight']
        return capacities, latencies

    def assign_default_capacity_and_latency(self):
        latencies = {}
        capacities = {}
        for n, nbrs in self.graph.adjacency_iter():
            for nbr, eattr in nbrs.items():
                latencies[(n, nbr)] = constants.NW_LATENCY_BASE
                capacities[(n, nbr)] = constants.DEFAULT_CAP
        return capacities, latencies

    def edge_switches(self):
        # Don't really know what is the edge anyway
        return list(self.graph.nodes())

    def get_shortest_paths(self, src, dst, with_weight=True):
        if with_weight:
            return nx.all_shortest_paths(self.graph,source=src,target=dst, weight='delay')
        else:
            return nx.all_shortest_paths(self.graph,source=src,target=dst)


    def get_shortest_path(self, src, dst, path_selector):
        """Returns a shortest path between src and dst

        Args:
            src: source switch
            dst: destination switch
            path_selector: controls which path is returned.
                float, 0 <= path_selector < 1

        Returns:
            path as a list of switches
        """
        paths = self.get_shortest_paths(src, dst)
        if not paths:
            return None
        paths = list(paths)
        assert 0 <= path_selector < 1
        return paths[int(path_selector * len(paths))]

    # TODO: get a random path, not just the shortest path

    def draw(self):
        """Draw the topology"""
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            log.warning("matplotlib could not be found")
            return
        node_color = range(len(self.graph.nodes()))

        pos = nx.spring_layout(self.graph,iterations=200)
        nx.draw(self.graph,pos,node_color=node_color,
                node_size=[100*(nx.degree(self.graph,x)**1.25) for x in self.graph.nodes()],
                edge_color=['blue' for x,y,z in self.graph.edges(data=True)],
                edge_cmap=plt.cm.Blues,
                with_labels=True,
                cmap=plt.cm.Blues)
        plt.show()
