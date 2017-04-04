import argparse
import os
import random
from random import Random

from misc import logger
from domain.network_premitives import *
from flow_gen.link_failure_change_generator import LinkFailureChangeGenerator
from flow_gen.random_change_generator import RandomChangeGenerator

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ez-segway sim.')
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
                        type=str, default="adjacency")
    parser.add_argument('--generating_method', nargs='?',
                        type=str, default=constants.RANDOM_GENERATION)
    parser.add_argument('--number_of_flows', nargs='?',
                        type=int, default=1000)
    parser.add_argument('--number_of_tests', nargs='?',
                        type=int, default=10)
    parser.add_argument('--failure_rate', nargs='?',
                        type=float, default=10)
    parser.add_argument('--path_generator', nargs='?',
                        type=str, default=constants.THIRD_SWITCH_GENERATION)
    parser.add_argument('--seed', nargs='?',
                        type=int, default=0)
    args = parser.parse_args()

    random.seed(args.seed)

    directory = "../%s" % (args.logFolder)
    if not os.path.exists(directory):
        os.makedirs(directory)

    logger.init("../" + args.logFolder + "/" + args.logFile, constants.LOG_LEVEL)
    log = logger.getLogger("data-generator", constants.LOG_LEVEL)
    log.info("---> Log start <---")

    if args.generating_method == constants.LINK_FAILURE_GENERATION:
        flow_change_generator = LinkFailureChangeGenerator(Random(42), args.failure_rate)
    else:
        flow_change_generator = RandomChangeGenerator(Random(42), args.path_generator)
    flow_change_generator.no_of_middleboxes = 1
    flow_change_generator.create_continuously_series_of_flows(args, log)
