import argparse
import os

from os import listdir
from os.path import isfile, join
from misc import logger
from flow_gen.flow_change_generator import FlowChangeGenerator
from misc import constants

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ez-segway sim.')
    parser.add_argument('--logFolder', nargs='?',
                        type=str, default="logs")
    parser.add_argument('--logFile', nargs='?',
                        type=str, default="stdout")
    parser.add_argument('--data_file', nargs='?',
                        type=str, default="data")
    parser.add_argument('--end_flow', nargs='?',
                        type=str, default="data")
    args = parser.parse_args()

    directory = "../%s" % (args.logFolder)
    if not os.path.exists(directory):
        os.makedirs(directory)

    logger.init("../" + args.logFolder + "/" + args.logFile, constants.LOG_LEVEL)
    log = logger.getLogger("data-generator", constants.LOG_LEVEL)
    log.info("---> Log start <---")

    flow_gen = FlowChangeGenerator()
    filename = "../%s" % args.data_file
    update = flow_gen.read_flows(filename)
    for flow in update.old_flows:
        flow.path = []
    flow_gen.write_flows_pair(filename, update.old_flows, update.new_flows)

    filename = "../%s" % args.end_flow
    update = flow_gen.read_flows(filename)
    for flow in update.new_flows:
        flow.path = []
    flow_gen.write_flows_pair(filename, update.old_flows, update.new_flows)
