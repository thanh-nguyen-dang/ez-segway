import os
import itertools
import random

from misc import logger, utils, global_vars, constants
from misc.utils import FlowSrcDst
from domain.network_premitives import GenSingleFlow, GenMulFlow, NetworkUpdate, NetworkUpdateInfo
from path_generators import PathGenerator
from ez_lib import ez_flow_tool
from ez_lib.ez_topo import Ez_Topo
from collections import defaultdict, deque
from copy import deepcopy, copy
from itertools import izip


class MulFlowChangeGenerator(object):
    def __init__(self, path_generator=None, rng=random.Random()):
        self.rng = rng
        self.log = self.init_logger()
        if path_generator is None:
            path_generator = PathGenerator()
        self.path_generator = path_generator
        self.pairs = []
        self.no_of_middleboxes = 0

    def random_src_dst_gen(self, switch_list):
        while True:
            src_switch = self.rng.choice(switch_list)
            if src_switch == max(switch_list):
                continue
            dst_switch = self.rng.choice(switch_list)
            while dst_switch <= src_switch:
                dst_switch = self.rng.choice(switch_list)
            yield src_switch, dst_switch

    def get_src_dst(self):
        pass

    def ordering_src_dst(self, switch_list, tm):
        list_src_dst = []
        for src in xrange(len(switch_list)):
            for dst in xrange(src + 1, len(switch_list)):
                list_src_dst.append(FlowSrcDst(src, dst, tm[src][dst], tm[dst][src]))
        list_src_dst.sort()
        return list_src_dst

    @staticmethod
    def init_logger():
        return logger.getLogger("flow_generator", constants.LOG_LEVEL)

    def generate_traffic_matrix(self, id_nodes):
        tm = {}
        t_in = {}
        t_out = {}
        total_in = 0
        total_out = 0

        # generate the first N_1 values as random exponential variables
        for i in range(0, len(id_nodes) - 1):
            n = id_nodes[i]
            t_in[n] = random.expovariate(1)
            t_out[n] = random.expovariate(1)
            total_in += t_in[n]
            total_out += t_out[n]

        # adjust the matrix with the last element
        last = id_nodes[len(id_nodes) - 1]
        if total_in > total_out:
            t_in[last] = random.expovariate(1)
            total_in += t_in[last]
            t_out[last] = total_in - total_out
            total_out += t_out[last]
        else:
            t_out[last] = random.expovariate(1)
            total_out += t_out[last]
            t_in[last] = total_out - total_in
            total_in += t_in[last]

        # print "\ninput vector: %s\noutput vector: %s\n
        # total input traffic: %d\ntotal output traffic: %d"
        # %(t_in, t_out, total_in, total_out)

        # compute the traffic matrix according to the gravity model, see equation (1) in
        # "Simplifying the synthesis of Internet Traffic Matrices", M. Roughan, in CCR 2005

        max_vol = 0
        for n in id_nodes:
            tm[n] = {}
            for m in id_nodes:
                traffic_vol = (t_in[n] * t_out[m]) / total_in
                tm[n][m] = traffic_vol
                if max_vol < traffic_vol:
                    max_vol = traffic_vol

        return tm, max_vol


    def normalize_by_unit(self, id_nodes, tm, max_vol):
        for n in id_nodes:
            for m in id_nodes:
                tm[n][m] = tm[n][m] * float(constants.MAX_CAP) / (1.5 * max_vol)


    def limit_difference(self, id_nodes, tm, max_vol):
        # limit the difference between the maximum and minimum volume
        nomalized_max_vol = max_vol * float(constants.MAX_CAP) / (1.5 * max_vol)
        nomalized_min_vol = nomalized_max_vol / constants.DIFF_SCALE
        for n in id_nodes:
            for m in id_nodes:
                if (tm[n][m] * constants.DIFF_SCALE < nomalized_max_vol):
                    tm[n][m] = nomalized_min_vol * self.rng.uniform(0.9, 2)


    def set_back_to_old_flow(self, new_flow, old_flow, link_caps):
        new_flow.path = old_flow.path
        new_flow.vol = old_flow.vol
        self.path_generator.allocate_link_cap(new_flow.path, link_caps, new_flow.vol, new_flow.reversed_vol)

    def compute_new_vol(self, old_vol):
        lower_bound, upper_bound = constants.DELTA_VOLUME
        delta = self.rng.uniform(lower_bound, upper_bound)
        new_vol = delta * old_vol
        while (new_vol >= constants.MAX_CAP):
            delta = self.rng.uniform(lower_bound, upper_bound)
            new_vol = delta * old_vol
        return new_vol

    @staticmethod
    def has_statistic_info(line):
        strs = line.strip("\n").split("\t")
        if len(strs) > 7:
            return True
        return False

    def read_statistic_info(self, flow_file):
        flow_reader = open(flow_file, 'r')
        line = flow_reader.readline()
        statistic_line = None
        has_statistic_line = self.has_statistic_info(line)
        if has_statistic_line:
            statistic_line = copy(line)
            network_update_info = NetworkUpdateInfo()
            network_update_info.set_statistic_info_from_string(statistic_line)
            return network_update_info
        return None

    def print_old_new_path(self, src, dst, old_vol, new_vol, old_p, new_p):
        end_points = str("(%d, %d)" % (src, dst))
        old_path_str = str("%f\t%s" % (old_vol, old_p))
        new_path_str = str("%f\t%s" % (new_vol, new_p))
        return str("%s\t%s\t%s\n" % (end_points, old_path_str, new_path_str))

    def print_flow(self, old_flow, new_flow):
        lines = ""
        for old_p, new_p in izip(old_flow.path, new_flow.path):
            if not old_p and not new_p:
                continue
            line = self.print_old_new_path(old_flow.src, old_flow.dst, old_flow.unit_vol, new_flow.unit_vol, old_p, new_p)
            lines += line

            reversed_old_p = list(reversed(old_p))
            reversed_new_p = list(reversed(new_p))
            line = self.print_old_new_path(old_flow.dst, old_flow.src, old_flow.unit_reversed_vol,
                                           new_flow.unit_reversed_vol,
                                           reversed_old_p, reversed_new_p)
            lines += line
        return lines

    def add_statistic_info(self, update, old_flows, new_flows, old_link_caps, new_link_caps):
        if len(old_link_caps) == 0:
            update.stat_info.min_old_utilizing = 0
            update.stat_info.max_old_utilizing = 0
            update.stat_info.avg_old_utilizing = 0
        else:
            update.stat_info.min_old_utilizing = constants.MAX_CAP - max(old_link_caps.values())
            update.stat_info.max_old_utilizing = constants.MAX_CAP - min(old_link_caps.values())
            update.stat_info.avg_old_utilizing = constants.MAX_CAP - (
                sum(old_link_caps.values()) / len(old_link_caps.values()))
        update.stat_info.free_old_link = len(global_vars.link_capacities.values()) - len(old_link_caps.values())

        if len(new_link_caps) == 0:
            update.stat_info.min_new_utilizing = 0
            update.stat_info.max_new_utilizing = 0
            update.stat_info.avg_new_utilizing = 0
        else:
            update.stat_info.min_new_utilizing = constants.MAX_CAP - max(new_link_caps.values())
            update.stat_info.max_new_utilizing = constants.MAX_CAP - min(new_link_caps.values())
            update.stat_info.avg_new_utilizing = constants.MAX_CAP - (
                sum(new_link_caps.values()) / len(new_link_caps.values()))
        update.stat_info.free_new_link = len(global_vars.link_capacities.values()) - len(new_link_caps.values())

        update.stat_info.no_of_segments_by_count = self.analyze_pivot_switches(old_flows, new_flows)

        # return "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%s\n" % (min_old_utilizing, max_old_utilizing,
        #                                              avg_old_utilizing, free_old_link,
        #                                              min_new_utilizing, max_new_utilizing,
        #                                              avg_new_utilizing, free_new_link, str_segment_no)


    def analyze_pivot_switches_for_flow(self, old_flow, new_flow, no_of_segments_by_flow_id):
        # to_sames, segs_length = ez_flow_tool.path_to_ops_by_link(old_flow.flow_id, None, None,
        #                                                          old_flow, new_flow)
        # if not no_of_segments_by_flow_id.has_key(segs_length):
        #     no_of_segments_by_flow_id[segs_length] = 1
        # else:
        #     no_of_segments_by_flow_id[segs_length] += 1
        pass


    def print_pivot_switches_info(self, no_of_segments_by_flow_id):
        count = 0
        sum = 0
        str_output = ""
        for key in no_of_segments_by_flow_id.keys():
            sum += key * no_of_segments_by_flow_id[key]
            str_output += "\t%d:%d" % (key, no_of_segments_by_flow_id[key])
            self.log.info("Number of flows having %d segment(s): %d" % (key, no_of_segments_by_flow_id[key]))
        # avg = sum/float(len(no_of_segments_by_flow_id.values()))
        # if count > 0:
        #     self.log.info("Average number of segments: %f" % avg)
        return str_output


    def write_flows(self, flow_file, update, write_reversed_flow=True):
        flow_writer = open(flow_file, 'w')
        str_statistic = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s%s\n" % (update.stat_info.min_old_utilizing,
                                                                update.stat_info.max_old_utilizing,
                                                                update.stat_info.avg_old_utilizing,
                                                                update.stat_info.free_old_link,
                                                                update.stat_info.min_new_utilizing,
                                                                update.stat_info.max_new_utilizing,
                                                                update.stat_info.avg_new_utilizing,
                                                                update.stat_info.free_new_link,
                                                                self.print_pivot_switches_info(
                                                                    update.stat_info.no_of_segments_by_count
                                                                ))
        flow_writer.write(str_statistic)
        str_flows = ""
        for old_flow, new_flow in itertools.izip(update.old_flows, update.new_flows):
            self.log.debug(old_flow)
            self.log.debug(new_flow)
            str_flows += self.print_flow(old_flow, new_flow)
        flow_writer.write(str_flows)
        flow_writer.close()

    def return_flows(self, old_flows, new_flows, old_link_caps, new_link_caps):
        ret_old_flows = []
        ret_new_flows = []
        for (src, dst) in old_flows.keys():
            if old_flows[(src, dst)].path == [] and new_flows[(src, dst)].path == []:
                new_flows.pop((src, dst), None)
                continue
            ret_old_flows.append(old_flows[(src, dst)])
            ret_new_flows.append(new_flows[(src, dst)])
        network_update = NetworkUpdate(ret_old_flows, ret_new_flows)
        self.add_statistic_info(network_update, network_update.old_flows, network_update.new_flows, old_link_caps, new_link_caps)
        return network_update

    def parse_args(self, args, log):
        data_directory = "../%s/%s" % (args.data_folder, args.topology)
        ez_topo = Ez_Topo()
        if args.topology_type == constants.TOPO_ROCKETFUEL:
            topo = ez_topo.create_rocketfuel_topology(data_directory)
        elif args.topology_type == constants.TOPO_ADJACENCY:
            topo = ez_topo.create_topology_from_adjacency_matrix(data_directory)
        elif args.topology_type == constants.TOPO_WEIGHTED_ADJACENCY:
            topo = ez_topo.create_latency_topology_from_adjacency_matrix(data_directory)
        else:
            raise Exception("What topology type")

        flow_folder = utils.get_flow_folder(data_directory, args.topology_type,
                                            args.generating_method, str(args.number_of_flows),
                                            str(args.failure_rate))
        return topo, flow_folder

    def get_to_try(self, topo, flow):
        tried = flow.src, flow.dst
        tries = [sw for sw in topo.edge_switches() if sw not in tried]
        return tries

    def generate_middleboxes(self, topo, flow):
        count = 0
        tries = self.get_to_try(topo, flow)
        while count < self.no_of_middleboxes:
            new_middlebox = self.rng.choice(tries)
            tries.remove(new_middlebox)
            count += 1
            flow.mdbxes.append(new_middlebox)

    def generate_one_state(self, topo, tm, flow_cnt):
        src_dst_queue = deque(deepcopy(self.pairs))

        # src_dst_gen = self.random_src_dst_gen(topo.edge_switches())
        link_caps = defaultdict()
        flows = defaultdict()

        while len(src_dst_queue) > 0 and len(flows) < flow_cnt:
            flow_src_dst = src_dst_queue.pop()
            src = flow_src_dst.lt_id
            dst = flow_src_dst.gt_id
            vol = flow_src_dst.vol
            while len(src_dst_queue) > 0 and (flows.has_key((src, dst)) or tm[src][dst] == 0
                                              or not self.path_generator.check_eligible_src_dst(topo, link_caps,
                                                                                                src, dst,
                                                                                                tm[src][dst],
                                                                                                tm[dst][src])):
                flow_src_dst = src_dst_queue.popleft()
                src = flow_src_dst.lt_id
                dst = flow_src_dst.gt_id
                vol = flow_src_dst.vol

            flow = GenMulFlow(len(flows), src, dst, vol, update_type=constants.ADDING_FLOW,
                                 reversed_vol=flow_src_dst.reversed_vol)

            self.path_generator.attempts = len(topo.edge_switches())
            has_path = self.generate_multiple_path(topo, flow, link_caps, constants.NO_MULT_PATH)
            if not has_path:
                continue
            flows[(src, dst)] = flow
            flows[(src, dst)].path = [p for p in flows[(src, dst)].path if p]
        return flows, link_caps

    def generate_multiple_path(self, topo, flow, link_caps, path_count):
        unit_vol = flow.unit_vol
        reversed_unit_vol = flow.unit_reversed_vol
        count = 0
        for i in xrange(0, path_count):
            temp_flow = GenSingleFlow(flow.flow_id, flow.src, flow.dst, unit_vol,
                                      flow.update_type, reversed_unit_vol)
            path = self.path_generator.generate_path(topo, temp_flow, link_caps)
            if path:
                count += 1
            flow.path.append(path)
        if count < path_count and count > 0:
            print(flow)
            print("flow %s->%s vol before: %s" % (flow.src, flow.dst, flow.vol))
            print("flow %s->%s reversed_vol before: %s" % (flow.dst, flow.src, flow.reversed_vol))
        flow.vol = unit_vol * count
        flow.reversed_vol = reversed_unit_vol * count
        if count < path_count and count > 0:
            print("flow %s->%s vol after: %s" % (flow.src, flow.dst, flow.vol))
            print("flow %s->%s reversed_vol after: %s" % (flow.dst, flow.src, flow.reversed_vol))
        return count > 0

    def generate_one_state_from_old(self, topo, tm, flow_cnt, old_flows):
        src_dst_queue = deque(deepcopy(self.pairs))
        link_caps = defaultdict()
        flows = defaultdict()
        empty_path_count = 0

        for (src, dst) in old_flows.keys():
            flow = deepcopy(old_flows[(src, dst)])
            flow.vol = self.compute_new_vol(flow.vol)
            flow.reversed_vol = self.compute_new_vol(flow.reversed_vol)

            src_dst_queue.remove(FlowSrcDst(src, dst, tm[src][dst], tm[dst][src]))
            is_old_no_path = (flow.path == [])

            path_count = flow.non_empty_path_count
            flow.path = []
            self.path_generator.attempts = len(topo.edge_switches())

            has_path = self.generate_multiple_path(topo, flow, link_caps, path_count)
            if has_path or (not has_path and not is_old_no_path):
                if not has_path and not is_old_no_path:
                    update_type = constants.REMOVING_FLOW
                flows[(src, dst)] = flow
                current_path_count = len(flow.path)
                current_non_empty_path_count = flow.non_empty_path_count
                if current_path_count < path_count:
                    flow.path = [p for p in flow.path if p]
                    for i in range(0, current_path_count - current_non_empty_path_count):
                        flow.path.append([])
            elif not has_path and is_old_no_path:
                old_flows.pop((src, dst))
            if not has_path:
                empty_path_count += 1

        while len(src_dst_queue) > 0 and empty_path_count > 0:
            flow_src_dst = src_dst_queue.popleft()
            src = flow_src_dst.lt_id
            dst = flow_src_dst.gt_id
            vol = flow_src_dst.vol

            flow = GenMulFlow(len(flows), src, dst, vol,
                                 update_type = constants.ADDING_FLOW,
                                 reversed_vol= flow_src_dst.reversed_vol)
            self.generate_middleboxes(topo, flow)
            self.path_generator.attempts = len(topo.edge_switches())
            has_path = self.generate_multiple_path(topo, flow, link_caps, constants.NO_MULT_PATH)
            if has_path:
                old_flow = GenMulFlow(len(flows), src, dst, vol, reversed_vol=flow_src_dst.reversed_vol)
                flows[(src, dst)] = flow
                old_flows[(src, dst)] = old_flow
                empty_path_count -= 1
        return flows, link_caps

    @staticmethod
    def generate_empty_state(flows):
        empty_flows = defaultdict()
        link_caps = {}
        for (src, dst) in flows.keys():
            flow = deepcopy(flows[(src, dst)])
            flow.path = []
            for i in xrange(0, len(flows[(src, dst)].path)):
                flow.path.append([])
            empty_flows[(src, dst)] = flow
        for o_pair in global_vars.link_capacities.keys():
            link_caps[o_pair] = global_vars.link_capacities[o_pair]

        return empty_flows, link_caps

    def create_continuously_series_of_flows(self, args, log):
        topo, flow_folder = self.parse_args(args, log)

        if not os.path.exists(flow_folder):
            os.makedirs(flow_folder)

        # begin generate traffix matrix, normalize and limit the difference
        tm, max_vol = self.generate_traffic_matrix(topo.edge_switches())
        self.normalize_by_unit(topo.edge_switches(), tm, max_vol)
        self.limit_difference(topo.edge_switches(), tm, max_vol)
        # end generate traffix matrix, normalize and limit the difference

        self.pairs = self.ordering_src_dst(topo.edge_switches(), tm)
        self.log.info(tm)
        new_dict_flows, new_link_caps = self.generate_one_state(topo, tm, args.number_of_flows)
        old_dict_flows, old_link_caps = self.generate_empty_state(new_dict_flows)

        flow_file = "%s/%s_0.intra" % (flow_folder, constants.FLOW_FILE_NAME)
        network_update = self.return_flows(old_dict_flows, new_dict_flows, old_link_caps, new_link_caps)
        self.write_flows(flow_file, network_update)
        number_of_updates = 1

        while number_of_updates < args.number_of_tests:
            self.log.info("number of update: %d" % number_of_updates)
            old_dict_flows = deepcopy(new_dict_flows)
            old_link_caps = deepcopy(new_link_caps)
            dict_flows, link_caps = \
                self.generate_one_state_from_old(topo, tm, args.number_of_flows, old_dict_flows)
            network_update = self.return_flows(old_dict_flows, dict_flows, old_link_caps, link_caps)

            # if self.check_update_before_writing(network_update, tm):
            flow_file = "%s/%s_%s.intra" % (flow_folder, constants.FLOW_FILE_NAME, str(number_of_updates))
            # network_update = self.return_flows(old_dict_flows, new_dict_flows, old_link_caps, new_link_caps)
            self.write_flows(flow_file, network_update)
            number_of_updates += 1
            new_dict_flows = dict_flows
            new_link_caps = link_caps

    def write_flows_pair(self, flow_file, old_flows, new_flows):
        flow_writer = open(flow_file, 'w')
        flow_writer.write('something\n')
        str_flows = ""
        for old_flow, new_flow in itertools.izip(old_flows, new_flows):
            self.log.debug(old_flow)
            self.log.debug(new_flow)
            if old_flow.path != [] or new_flow.path != []:
                str_flows += self.print_flow(old_flow, new_flow)
        flow_writer.write(str_flows)
        flow_writer.close()

    def analyze_pivot_switches(self, old_flows, new_flows):
        no_of_segments_by_count = {}
        for old_flow, new_flow in itertools.izip(old_flows, new_flows):
            self.analyze_pivot_switches_for_flow(old_flow, new_flow, no_of_segments_by_count)

        return no_of_segments_by_count
        # self.print_pivot_switches_info(no_of_segments_by_count)
