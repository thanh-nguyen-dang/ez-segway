from misc import global_vars, constants, logger
from domain import topology


class Ez_Topo(object):
    def __init__(self):
        self.log = logger.getLogger("ez_topo", constants.LOG_LEVEL)

    def create_rocketfuel_topology(self, data_directory):
        topo = topology.Topology.read_rocketfuel_latencies(data_directory + "/" + constants.LATENCIES_FILE, \
                                                           data_directory + "/" + constants.WEIGHTS_FILE)
        global_vars.link_capacities, global_vars.sw_to_sw_delays = topo.export_rocketfuel_capacity_and_latency()

        id, degree = topo.get_highest_degree_node()
        topo.deploy_controller(id, global_vars.sw_to_ctrl_delays)
        self.log.debug("link capacities: %s" % global_vars.link_capacities)
        self.log.debug("sw to sw delay: %s" % global_vars.sw_to_sw_delays)
        self.log.debug("sw to ctrl delay: %s" % str(global_vars.sw_to_ctrl_delays))
        return topo

    def create_topology_from_adjacency_matrix(self, data_directory):
        topo_file = data_directory + "/%s" % constants.ADJACENCY_FILE
        topo = topology.Topology.read_adjacency(topo_file)
        global_vars.link_capacities, global_vars.sw_to_sw_delays = topo.assign_default_capacity_and_latency()
        for sw in topo.graph.nodes():
            global_vars.sw_to_ctrl_delays[sw] = 1
        return topo

    def create_latency_topology_from_adjacency_matrix(self, data_directory, control_sw=-1):
        topo_file = data_directory + "/%s" % constants.LATENCIES_FILE
        topo = topology.Topology.read_weighted_adjacency(topo_file)
        global_vars.link_capacities, global_vars.sw_to_sw_delays = topo.export_capacity_and_latency()

        if (control_sw == -1):
            centroid_file = data_directory + "/%s" % constants.CENTROID_FILE
            control_sw = self.read_centroid(centroid_file)
        topo.deploy_controller(control_sw, global_vars.sw_to_ctrl_delays)
        return topo

    def read_centroid(self, filename):
        f = open(filename)
        line = f.readline()
        f.close()
        return int(line)
