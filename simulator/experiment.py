# import SimPy.SimulationTrace
import argparse
import os
import logging
import time
import datetime
import signal

import SimPy.Simulation as Simulation

from devices import ez_segway_switch, centralized_controller, ez_segway_controller, centralized_switch
from domain import topology
from misc import logger, utils, constants, global_vars
from flow_gen.flow_change_generator import FlowChangeGenerator
from ez_lib.ez_topo import Ez_Topo


class EzSegway(Simulation.Process):
    def __init__(self, ctrl, topo):
        self.log = logger.getLogger("EzSegway", constants.LOG_LEVEL)
        self.log.debug("Create EzSegway")

        self.ctrl = ctrl
        self.topo = topo
        self.old_flows = []
        self.new_flows = []

        Simulation.Process.__init__(self, name='EzSegway')

    def __repr__(self):
        return "EzSegway"

    def load_flows_for_test(self, flows_file):
        debug = self.log.debug
        flow_gen = FlowChangeGenerator()
        update = flow_gen.read_flows(flows_file, False)
        self.old_flows = update.old_flows
        self.new_flows = update.new_flows
        # debug("old flows: %s" % self.old_flows)
        # debug("new flows: %s" % self.new_flows)

    def run(self):
        debug = self.log.debug
        info = self.log.info
        warning = self.log.warning
        critical = self.log.critical

        yield Simulation.hold, self,
        self.ctrl.install_update(self.old_flows, self.new_flows)

        # TODO: iterate a few times to install updates and wait until the controller reports that the update is finished


def sigterm_handler(_signo, _stack_frame):
    log.warning("SIGTERM caught")
    Simulation.stopSimulation()


def sigint_handler(_signo, _stack_frame):
    log.warning("SIGINT caught")
    Simulation.stopSimulation()


def create_topology(args, data_directory):
    ez_topo = Ez_Topo()
    if args.topology_type == constants.TOPO_ROCKETFUEL:
        return ez_topo.create_rocketfuel_topology(data_directory)
    elif args.topology_type == constants.TOPO_ADJACENCY:
        return ez_topo.create_topology_from_adjacency_matrix(data_directory)


def create_ctrl(args, topo):
    if args.method == constants.CENTRALIZED_METHOD:
        return centralized_controller.CentralizedController()
    elif args.method == constants.P2P_METHOD:
        return ez_segway_controller.P2PController()


def create_sw(args, topo, ctrl):
    switches = []

    for node in topo.graph.nodes():
        if args.method == constants.CENTRALIZED_METHOD:
            sw = centralized_switch.CentralizedSwitch(node, ctrl, sorted(topo.graph.neighbors(node)))
        elif args.method == constants.P2P_METHOD:
            sw = ez_segway_switch.EzSegwaySwitch(node, ctrl, sorted(topo.graph.neighbors(node)))
        Simulation.activate(sw, sw.run(), at=0.0)
        switches.append(sw)
    global_vars.switch_ids = sorted(topo.graph.nodes())
    return sorted(switches), sorted(topo.graph.nodes())


def run_one_test(args, log, ezsegway, flow_file):
    ezsegway.load_flows_for_test(flow_file)
    # Setup signal handlers
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGINT, sigint_handler)

    start = time.clock()
    global_vars.start_time = start
    # Begin simulation
    Simulation.simulate(until=args.simulationDuration)
    finish = time.clock()
    delta = datetime.timedelta(seconds=(finish - start))
    computation_time = datetime.timedelta(seconds=(global_vars.finish_computation_time - start))
    prioritizing_time = datetime.timedelta(seconds=(global_vars.finish_prioritizing_time -
                                                    global_vars.finish_computation_time))
    log.info("Simulation duration: %s" % Simulation.now())
    log.info("Simulation finished in %s" % str(delta))
    log.info("Computation finished in %s" % str(computation_time))
    log.info("Prioritizing finished in %s" % str(prioritizing_time))
    return Simulation.now()


def run_experiment(args, log):
    data_directory = "../%s/%s" % (args.data_folder, args.topology)
    topo = create_topology(args, data_directory)

    # topo.draw()

    ctrl = create_ctrl(args, topo)
    ezsegway = EzSegway(ctrl, topo)

    global_vars.ctrl = ctrl
    global_vars.switches, global_vars.switch_ids = create_sw(args, topo, ctrl)

    Simulation.activate(global_vars.ctrl, global_vars.ctrl.run(), at=0.0)
    Simulation.activate(ezsegway, ezsegway.run(), at=0.0)

    flow_folder = utils.get_flow_folder(data_directory, args.topology_type, args.generating_method,
                                        args.number_of_flows, args.failure_rate)

    update_time = run_one_test(args, log, ezsegway, flow_folder + "/flows_%d.intra" % args.test_number)
    with open("../results.dat", "a") as f:
        res = [args.method, args.topology, args.generating_method, args.failure_rate, args.number_of_flows,
               args.test_number, update_time]
        res = [str(x) for x in res]
        f.write(','.join(res) + "\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ez-segway sim.')
    parser.add_argument('--simulationDuration', nargs='?',
                        type=int, default=5000)
    parser.add_argument('--logFolder', nargs='?',
                        type=str, default="logs")
    parser.add_argument('--logLevel', nargs='?',
                        type=str, default="INFO")
    parser.add_argument('--logFile', nargs='?',
                        type=str, default="stdout")
    parser.add_argument('--data_folder', nargs='?',
                        type=str, default="data")
    parser.add_argument('--topology', nargs='?',
                        type=str, default="1755")
    parser.add_argument('--topology_type', nargs='?',
                        type=str, default=constants.TOPO_ROCKETFUEL)
    parser.add_argument('--method', nargs='?',
                        type=str, default=constants.P2P_METHOD)
    parser.add_argument('--generating_method', nargs='?',
                        type=str, default=constants.LINK_FAILURE_GENERATION)
    parser.add_argument('--test_number', nargs='?',
                        type=int, default=10)
    parser.add_argument('--number_of_flows', nargs='?',
                        type=int, default=1000)
    parser.add_argument('--failure_rate', nargs='?',
                        type=float, default=0.5)
    args = parser.parse_args()

    directory = "../%s" % (args.logFolder)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # numeric_level = getattr(logging, args.logLevel.upper(), None)
    # if not isinstance(numeric_level, int):
    #     raise ValueError('Invalid log level: %s' % numeric_level)
    # constants.LOG_LEVEL = numeric_level

    logger.init("../" + args.logFolder + "/" + args.logFile, constants.LOG_LEVEL)
    log = logger.getLogger("ez-segway", constants.LOG_LEVEL)
    log.info("---> Log start <---")

    run_experiment(args, log)
