import argparse
import os

from misc import logger
from misc import constants
from collections import defaultdict, OrderedDict
import numpy
import re

class ExecutionResult:
    def __init__(self):
        self.test_number = 0
        self.execution_time = ExecutionTime()

class ExecutionTime:
    def __init__(self, topo, method):
        self.test_number = 0
        self.total_time = 0
        self.method = method
        self.topo = topo
        self.global_computation = 0
        self.local_computation = 0
        self.local_update_only = 0
        self.finishing_time_from_last_sending = 0
        self.time_to_send_all_first_msgs = 0
        self.rule_overheads = 0
        self.total_rules = 0
        self.deadlock = False
        self.time_using_new_paths = {}

    def __str__(self):
        # arg_method,arg_topology,update_time,sw_time,ctr_time,update_only
        return "%s\t%s\t%s\t%s\t%s\t%s" % (self.method, self.topo, self.total_time,
                                           self.local_computation, self.global_computation,
                                           self.local_update_only)

    def __repr__(self):
        return self.__str__()

class MessageOverhead:
    def __init__(self):
        self.false_positive = False
        self.no_msgs_when_deadlock = 0
        self.no_msgs_when_split = 0
        self.stuck_into_deadlock_when_split = False
        self.stuck_into_deadlock_when_skip = False

    def __str__(self):
        return "%d\t%d\t%s" % (self.no_msgs_when_deadlock, self.no_msgs_when_split, self.false_positive)

    def __repr__(self):
        return self.__str__()

class NewPathPair:
    def __init__(self):
        self.p2p = 0
        self.cen = 0

    def __str__(self):
        return "cen:%d\tp2p:%d" % (self.cen, self.p2p)

    def __repr__(self):
        return self.__str__()

class RuleOverhead:
    def __init__(self):
        self.no_of_split = 0
        self.total_no_of_flow = 0
        self.stuck_into_deadlock_when_split = False
        self.stuck_into_deadlock_when_skip = False
        # self.rules_when_deadlock = 0
        # self.rules_when_split = 0

    def __str__(self):
        return "%d\t%d\t%s" % (self.no_of_split, self.total_no_of_flow,
                               (self.stuck_into_deadlock_when_split and
                                not self.stuck_into_deadlock_when_skip))

    def __repr__(self):
        return self.__str__()


class EzTracer:
    def __init__(self):
        self.execution_time_by_test_number = defaultdict()
        self.rule_overheads = {}
        self.execution_results = defaultdict()
        self.cdf_time_using_new_path = defaultdict()
        self.is_reading_deadlock_exe = False
        self.log = logger.getLogger("tracer", constants.LOG_LEVEL)
        self.number_of_rules_by_test_number = defaultdict()
        self.message_overheads = {}
        self.current_method = ''
        self.current_topo = ''
        self.counters = defaultdict()

    def parse_execution_line(self, line):
        strs = re.split("[:\t]+", line.strip('\n'))
        if len(strs) < 3:
            return None, None
        labels = strs[0].split('-')
        if len(labels) > 2:
            return None, None
        test_number = int(labels[1])

        exe_time = self.get_exe_time(test_number)
        exe_time.local_computation = float(strs[2])
        exe_time.global_computation = float(strs[3])
        exe_time.global_computation += exe_time.local_computation
        exe_time.total_time = float(strs[12])# + exe_time.global_computation
        exe_time.local_computation = 0
        exe_time.local_update_only = float(strs[4])
        exe_time.finishing_time_from_last_sending = float(strs[10])
        exe_time.method = self.current_method
        exe_time.topo = self.current_topo
        exe_time.deadlock = (strs[11] == "True")
        exe_time.rule_overheads = int(strs[5])
        exe_time.total_rules = int(strs[6])

        self.update_exe_time(exe_time, test_number)

        msg_overhead = None
        if not self.message_overheads.has_key(test_number):
            if (self.is_reading_deadlock_exe and strs[7] == 'True') \
                    or (not self.is_reading_deadlock_exe):
                self.message_overheads[test_number] = MessageOverhead()
                msg_overhead = self.message_overheads[test_number]
        else:
            msg_overhead = self.message_overheads[test_number]
        if msg_overhead is not None:
            # self.log.info("Test number %d: %s" % (test_number, strs))
            # self.log.info("Test number %d: %s" % (test_number, msg_overhead))
            if self.is_reading_deadlock_exe:
                msg_overhead.no_msgs_when_deadlock = float(strs[8])
                msg_overhead.stuck_into_deadlock_when_skip = True if strs[7] == "True" else False
            else:
                msg_overhead.no_msgs_when_split = float(strs[8])
                msg_overhead.stuck_into_deadlock_when_split = True if strs[7] == "True" else False

        return exe_time, msg_overhead

    def parse_centralized_execution_line(self, line):
        strs = re.split("[:\t]+", line.strip('\n'))
        if len(strs) < 3:
            return None
            # if line.find("deadlock"):
            #     labels = strs[0].split('-')
            #     test_number = int(labels[1])
            #     key = (self.current_topo, self.current_method)
            #     exe_time = self.execution_time_by_test_number[test_number][key]
            #     exe_time.deadlock = True
            #     return None
            # else:
            #     return None
        labels = strs[0].split('-')
        if len(labels) > 2:
            return None
        test_number = int(labels[1])

        exe_time = self.get_exe_time(test_number)
        exe_time.local_computation = float(strs[2])
        exe_time.global_computation = float(strs[3])
        exe_time.total_time = float(strs[5]) + exe_time.global_computation
        #exe_time.local_update_only = float(strs[4])
        #exe_time.finishing_time_from_last_sending = float(strs[5])
        exe_time.local_update_only = float(strs[5])
        exe_time.time_to_send_all_first_msgs = float(strs[6])
        exe_time.method = self.current_method
        exe_time.topo = self.current_topo
        # self.log.info("{0} - str[8]: {1}".format(test_number, strs[8]))
        exe_time.deadlock = (strs[8] == "True")

        self.update_exe_time(exe_time, test_number)

        return exe_time

    def update_exe_time(self, exe_time, test_number):
        if not self.execution_time_by_test_number.has_key(test_number):
            self.execution_time_by_test_number[test_number] = {}
        self.execution_time_by_test_number[test_number][(self.current_topo, self.current_method)] = exe_time

    def parse_split_overhead_line(self, strs):
        i = 5
        test_number = int(strs[1])
        while i < len(strs):
            overhead = RuleOverhead()
            overhead.no_of_split = int(strs[i])
            overhead.total_no_of_flow = int(strs[i+1])
            if not self.rule_overheads.has_key(test_number):
                self.rule_overheads[test_number] = overhead
            i += 2

    def parse_number_of_rules_line(self, strs):
        test_number = int(strs[1])
        sw_number = int(strs[3])
        if not self.number_of_rules_by_test_number.has_key(test_number):
            self.number_of_rules_by_test_number[test_number] = {}
        self.number_of_rules_by_test_number[test_number][sw_number] = int(strs[5])

    def parse_time_using_new_path_line(self, strs, exe_time):
        test_number = int(strs[1])
        i = 3 if exe_time.method == 'cen' else 5
        while i < len(strs):
            time_slot = int(float(strs[i + 1]))
            if not exe_time.time_using_new_paths.has_key(time_slot):
                exe_time.time_using_new_paths[time_slot] = 1
            else:
                exe_time.time_using_new_paths[time_slot] += 1
            i += 2

    def get_exe_time(self, test_number):
        key = (self.current_topo, self.current_method)
        if self.execution_time_by_test_number.has_key(test_number) and \
            self.execution_time_by_test_number[test_number].has_key(key):
                exe_time = self.execution_time_by_test_number[test_number][key]
        else:
            exe_time = ExecutionTime(self.current_topo, self.current_method)
            exe_time.test_number = test_number
        return exe_time

    def parse_line_of_every_switch(self, line):
        line = line.replace(" ", "")
        strs = filter(None, re.split("[:|\-\t\[\]\n]+", line))

        if len(strs) < 3 or strs[2] != "sw":
            return
        test_number = int(strs[1])
        exe_time = self.get_exe_time(test_number)
        self.update_exe_time(exe_time, test_number)
        if strs[4] == "new_path":
            self.parse_time_using_new_path_line(strs, exe_time)
        elif strs[4] == "split":
            strs = filter(None, re.split("[:|\-\t\[\]\,\n]+", line))
            self.parse_split_overhead_line(strs)
        elif strs[4] == "no_rules":
            self.parse_number_of_rules_line(strs)

    def parse_cen_line_of_every_switch(self, line):
        line = line.replace(" ", "")
        strs = filter(None, re.split("[:|\-\t\[\]\n]+", line))
        # self.log.info(strs)
        if len(strs) < 3:
            return
        if strs[2] == "new_path":
            test_number = int(strs[1])
            exe_time = self.get_exe_time(test_number)
            self.parse_time_using_new_path_line(strs, exe_time)

    def read(self, filename, skip_deadlock):
        self.is_reading_deadlock_exe = skip_deadlock
        trace_reader = open(filename, 'r')
        line = trace_reader.readline()
        while line:
            if line.startswith("ez-segway: read") or line.startswith("topology:"):
                line = trace_reader.readline()
                continue
            line = line.replace("ez-segway: ", "")
            exe_time, rule_overhead = self.parse_execution_line(line)
            if exe_time is None:
                self.parse_line_of_every_switch(line)
            line = trace_reader.readline()

        # for key in self.cdf_time_using_new_path.keys():
        #     self.cdf_time_using_new_path[key] = \
        #         OrderedDict(sorted(self.cdf_time_using_new_path[key].items(), key=lambda t: t[0]))

        # self.log.info(self.cdf_time_using_new_path)

        trace_reader.close()

    def read_centralized_trace(self, filename):
        self.is_reading_deadlock_exe = True
        trace_reader = open(filename, 'r')
        line = trace_reader.readline()
        while line:
            if line.startswith("cen_result"):
                line = line.replace("cen_result: ", "")
                exe_time = self.parse_centralized_execution_line(line)
                # self.log.info("exe_time=%s" % str(exe_time))
                if exe_time is None:
                    self.parse_cen_line_of_every_switch(line)
            line = trace_reader.readline()

        # for key in self.cdf_time_using_new_path.keys():
        #     self.cdf_time_using_new_path[key] = \
        #         OrderedDict(sorted(self.cdf_time_using_new_path[key].items(), key=lambda t: t[0]))

        # self.log.info(self.cdf_time_using_new_path)

        trace_reader.close()

    def write_rules_overhead(self):
        # trace_writer = open(folder + "/overhead_n.log", 'w')
        rule_overheads = []
        total_rules = []
        for key in self.execution_time_by_test_number:
            for exe_time in self.execution_time_by_test_number[key].values():
                if exe_time.deadlock and exe_time.method == 'split':
                    rule_overheads.append(exe_time.rule_overheads)
                    total_rules.append(exe_time.total_rules)

        max_rule_overhead = numpy.max(rule_overheads)
        average_rule_overhead = numpy.mean(rule_overheads)
        std = numpy.std(rule_overheads)
        max_total_rules = numpy.max(total_rules)
        self.log.info("Max rule overhead: %d in %d cases" % (max_rule_overhead, len(rule_overheads)))
        self.log.info("Sum overhead: %d" % sum(rule_overheads))
        self.log.info("Average rule overhead: %s +/- %s" % (average_rule_overhead, std))
        self.log.info("Max total rules: %d" % max_total_rules)

    def write_message_overhead(self, folder):
        trace_writer = open(folder + "/overhead_n.log", 'w')
        str_overheads = ""
        overheads = []
        total_message_no_overheads = []
        for key in self.message_overheads.keys():
            overhead = self.message_overheads[key]
            if overhead.stuck_into_deadlock_when_skip or \
                    overhead.stuck_into_deadlock_when_split:
                # self.log.info(overhead)
                diff = overhead.no_msgs_when_split - overhead.no_msgs_when_deadlock
                str_overheads += "Test number %d:%d\t%d\t%s\t%s\n" \
                                 % (key, diff,
                                    overhead.no_msgs_when_deadlock,
                                    overhead.stuck_into_deadlock_when_split,
                                    overhead.stuck_into_deadlock_when_skip)
                overheads.append(diff)
                total_message_no_overheads.append(overhead.no_msgs_when_deadlock)

        average = numpy.mean(overheads)
        max_overhead = numpy.max(overheads)
        std = numpy.std(overheads)

        average_total = numpy.mean(total_message_no_overheads)
        max_total = numpy.max(total_message_no_overheads)
        std_total = numpy.std(total_message_no_overheads)

        str_overheads += "Overhead average: %s +/- %s\n" % (average, std)
        str_overheads += "Max overhead: %d\n" % max_overhead
        str_overheads += "Sum overhead: %d of %d cases\n" % (sum(overheads), len(overheads))
        str_overheads += "Total number average: %s +/- %s\n" % (average_total, std_total)
        str_overheads += "Max message: %d\n" % max_total
        self.log.info(str_overheads)
        trace_writer.write(str_overheads)
        trace_writer.close()

        # trace_writer = open(folder + "/false_positive.log", 'w')
        # total_split_cases = len(self.rule_overheads)
        # false_positive_count = sum(1 for item in self.message_overheads.values() if item.false_positive == True)
        # str_false_positive_res = "False positive / Deadlock: %d / %d" % (false_positive_count, total_split_cases)
        # trace_writer.write(str_false_positive_res)
        # trace_writer.close()


    def write_execution_time(self, filename):
        self.log.info("output to file: %s" % filename)
        trace_writer = open(filename, 'w')
        str_output = "arg_method\targ_topology\tupdate_time\tsw_time\tctr_time\tupdate_only\n"
        for key in self.execution_time_by_test_number:
            for exe_time in self.execution_time_by_test_number[key].values():
                if (not exe_time.deadlock and exe_time.method == 'ez') or exe_time.method == 'cen':
                    str_output += str(exe_time) + '\n'
        trace_writer.write(str_output)
        trace_writer.close()

    def write_cdf_of_a_test_number(self, folder, test_number):
        trace_writer = open(folder + ("/time_new_path/time_using_new_path_%d.log" % test_number), 'w')
        cdf = self.cdf_time_using_new_path[test_number]
        sum = 0
        max_key = max(cdf.keys())
        cdf_res = "arg_method\tcount\n"
        for key in cdf.keys():
            if cdf[key].p2p > 0:
                for c in xrange(0, cdf[key].p2p):
                    cdf_res += "ez\t%d\n" % (key)
            if cdf[key].cen > 0:
                for c in xrange(0, cdf[key].cen):
                    cdf_res += "cen\t%d\n" % (key)
        trace_writer.write(cdf_res)
        trace_writer.close()

    def write_new_cdf_of_a_test_number(self, folder, test_number, topo):
        trace_writer = open(folder + ("/time_new_path/time_using_new_path_%d.log" % test_number), 'w')
        cdf_res = "arg_method\tcount\n"
        exe_list = self.execution_time_by_test_number[test_number].values()
        for exe_time in exe_list:
            if ((not exe_time.deadlock and exe_time.method == 'ez')
                or exe_time.method == 'cen') and exe_time.topo == topo:
                #if exe_time.topo == 'ez':
                cdf = exe_time.time_using_new_paths
                # self.log.info("%s: %s" % (exe_time.method, cdf.values()))
                for key in cdf.keys():
                    if cdf[key] > 0:
                        for c in xrange(0, cdf[key]):
                            cdf_res += "%s\t%d\n" % (exe_time.method,key)
        trace_writer.write(cdf_res)
        trace_writer.close()

    def write_cdf_of_new_path(self, folder):
        for key in self.cdf_time_using_new_path.keys():
            self.write_cdf_of_a_test_number(folder, key)

    def write_new_cdf_of_new_path(self, folder, topo):
        for key in self.execution_time_by_test_number.keys():
            self.write_new_cdf_of_a_test_number(folder, key, topo)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ez-segway sim.')
    parser.add_argument('--logFolder', nargs='?',
                        type=str, default="logs")
    parser.add_argument('--logFile', nargs='?',
                        type=str, default="stdout")
    parser.add_argument('--dataFolder', nargs='?',
                        type=str, default="data")
    args = parser.parse_args()

    directory = "../%s" % (args.logFolder)
    if not os.path.exists(directory):
        os.makedirs(directory)

    logger.init("../" + args.logFolder + "/" + args.logFile, constants.LOG_LEVEL)

    tracer = EzTracer()
    methods = ['cen','ez']
    topos = ['b4', 'i2']
    for topo in topos:
        for method in methods:
            tracer.current_method = method
            tracer.current_topo = topo
            filename = "../{0}/{1}-{2}.log".format(args.dataFolder, method, topo)
            if method == 'cen':
                tracer.read_centralized_trace(filename)
            else:
                tracer.read(filename, False)
    output_filename = "../{0}/update_time.log".format(args.dataFolder)
    tracer.write_execution_time(output_filename)
    for topo in topos:
        folder = "{0}/{1}-cdf".format(args.dataFolder, topo)
        tracer.write_new_cdf_of_new_path("../" + folder, topo)
