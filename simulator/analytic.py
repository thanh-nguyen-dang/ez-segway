import argparse
import os

from os import listdir
from os.path import isfile, join
from misc import logger
from flow_gen.flow_change_generator import FlowChangeGenerator
from flow_gen.random_change_generator import RandomChangeGenerator
from misc import constants

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ez-segway sim.')
    parser.add_argument('--logFolder', nargs='?',
                        type=str, default="logs")
    parser.add_argument('--logFile', nargs='?',
                        type=str, default="stdout")
    parser.add_argument('--data_folder', nargs='?',
                        type=str, default="data")
    args = parser.parse_args()

    directory = "../%s" % (args.logFolder)
    if not os.path.exists(directory):
        os.makedirs(directory)

    logger.init("../" + args.logFolder + "/" + args.logFile, constants.LOG_LEVEL)
    log = logger.getLogger("data-generator", constants.LOG_LEVEL)
    log.info("---> Log start <---")

    flow_change_generator = FlowChangeGenerator()
    datafiles = [f for f in listdir(args.data_folder) if isfile(join(args.data_folder, f))]
    # for file in datafiles:
    #     update = flow_change_generator.read_flows(join(args.data_folder, file))
    #     update.no_of_segments_by_count = \
    #         flow_change_generator.analyze_pivot_switches(update.old_flows, update.new_flows)
    #     flow_change_generator.write_flows(join(args.data_folder, file), update, write_reversed_flow=False)

    avg_of_avg_old = 0
    avg_of_avg_new = 0
    count = 0
    for file in datafiles:
        network_update_info = flow_change_generator.read_statistic_info(join(args.data_folder, file))
        avg_of_avg_old += network_update_info.avg_old_utilizing
        avg_of_avg_new += network_update_info.avg_new_utilizing
        count += 1
    avg_of_avg_old = float(avg_of_avg_old) / count
    avg_of_avg_new = float(avg_of_avg_new) / count

    stat_file = join(args.data_folder, "stat_info.intra")
    info_writer = open(stat_file, 'w')
    log.info("%s\t%s\n" % (avg_of_avg_old, avg_of_avg_new))
    info_writer.write("%s\t%s\n" % (avg_of_avg_old, avg_of_avg_new))
