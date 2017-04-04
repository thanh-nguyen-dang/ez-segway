import itertools
from misc import logger, constants, global_vars
import networkx as nx
from collections import deque, defaultdict
from copy import copy
from k_shortest_paths import k_shortest_paths

class PathGenerator(object):
    def __init__(self, composed_generation=True):
        self.log = self.init_logger()
        self.composed_generation = composed_generation

    @staticmethod
    def init_logger():
        print("create logger for path_generator")
        return logger.getLogger("path_generator", constants.LOG_LEVEL)

    def generate_path(self, topo, flow, link_caps):
        pass

    @staticmethod
    def calculate_latency(path):
        latency = 0
        for i, j in itertools.izip(path[0:len(path)-1], path[1:len(path)]):
            latency += global_vars.sw_to_sw_delays[(i, j)]
        return latency

    def check_available_cap(self, topo, link_caps, n, vol, reversed_vol):
        nbrs = topo.graph.neighbors(n)
        for nbr in nbrs:
            if self.check_capacity_for_link(topo, link_caps, n, nbr, vol) \
                    and self.check_capacity_for_link(topo, link_caps, nbr, n, reversed_vol):
                return True
        return False

    def check_eligible_src_dst(self, topo, link_caps, src, dst, vol, reversed_vol):
        return dst not in topo.graph.neighbors(src) \
                and self.check_available_cap(topo, link_caps, src, vol, reversed_vol) \
                and self.check_available_cap(topo, link_caps, dst, vol, reversed_vol)

    def check_capacity_for_link(self, topo, link_caps, src, dst, vol):
        pair = (src, dst)
        if pair not in link_caps.keys():
            link_caps[pair] = global_vars.link_capacities[pair]
        cap = link_caps[pair]
        return cap >= vol

    def check_capacity(self, topo, part, link_caps, vol, reversed_vol):
        for src, dst in itertools.izip(part[0:len(part) - 1], part[1:len(part)]):
            if not self.check_capacity_for_link(topo, link_caps, src, dst, vol) or \
                    not self.check_capacity_for_link(topo, link_caps, dst, src, reversed_vol):
                return False
        return True

    def checking_joint(self, first_seg, sec_seg):
        cycle = False
        for v in sec_seg:
            if v in first_seg:
                return True
        return False

    @staticmethod
    def allocate_link_cap(part, link_caps, vol, reversed_vol):
        for src, dst in itertools.izip(part[0:len(part) - 1], part[1:len(part)]):
            if (src, dst) not in link_caps.keys():
                link_caps[(src, dst)] = global_vars.link_capacities[(src, dst)] - vol
            else:
                link_caps[(src, dst)] -= vol
            if (dst, src) not in link_caps.keys():
                link_caps[(dst, src)] = global_vars.link_capacities[(dst, src)] - reversed_vol
            else:
                link_caps[(dst, src)] -= reversed_vol
            if link_caps[(src, dst)] < 0 or link_caps[(dst, src)] < 0:
                str_src_dst = "{0}->{1}".format(src, dst)
                str_dst_src = "{0}->{1}".format(dst, src)
                raise Exception("Link {0} has capacity: {1}. And Link {2} has capacity {3}."
                                .format(str_src_dst, link_caps[(src, dst)],
                                        str_dst_src, link_caps[(dst, src)]))

    @staticmethod
    def rollback_link_cap(part, link_caps, vol, reversed_vol):
        for src, dst in itertools.izip(part[0:len(part) - 1], part[1:len(part)]):
            link_caps[(src, dst)] += vol
            if link_caps[(src, dst)] == global_vars.link_capacities[(src, dst)]:
                link_caps.pop((src, dst))
            link_caps[(dst, src)] += reversed_vol
            if link_caps[(dst, src)] == global_vars.link_capacities[(dst, src)]:
                link_caps.pop((dst, src))
        return True


class ShortestPathGenerator(PathGenerator):
    def __init__(self, rng):
        super(ShortestPathGenerator, self).__init__()
        self.rng = rng

    def generate_path(self, topo, flow, link_caps):
        try:
            shortest_paths = [p for p in topo.get_shortest_paths(flow.src, flow.dst)]
        except nx.exception.NetworkXNoPath:
            self.log.debug("No shortest path for (%d, %d)" % (flow.src, flow.dst))
            return False
        for path in shortest_paths:
            if not self.check_capacity(topo, path, link_caps, flow.vol, flow.reversed_vol):
                continue
            else:
                self.allocate_link_cap(path, link_caps, flow.vol, flow.reversed_vol)
                flow.path = path
                break
        return flow.path, None


class ThirdSwitchPathGenerator(PathGenerator):
    def __init__(self, rng, third_sw=None, attempts=10, old_path=None):
        super(ThirdSwitchPathGenerator, self).__init__()
        self.rng = rng
        self.third_sw = third_sw
        self.attempts = attempts
        self.old_path = old_path

    def generate_path(self, topo, flow, link_caps):
        path = []
        count = 0
        self.log.debug("Flow%d: %d --> %d: %s" % (flow.flow_id, flow.src, flow.dst, str(flow.vol)))
        third = self.third_sw
        tried = self.third_sw, flow.src, flow.dst
        no_try = [sw for sw in topo.edge_switches() if sw not in tried]
        try:
            src_dst_shortest = [p for p in topo.get_shortest_paths(flow.src, flow.dst)]
            src_dst_latency = self.calculate_latency(src_dst_shortest[0])
            self.log.debug("Has path with latency %s" % src_dst_latency)
        except nx.exception.NetworkXNoPath:
            return []

        while len(path) == 0 and count < self.attempts - 3:
            if len(no_try) == 0:
                return []
            first_part_latency = 1.5 * src_dst_latency
            second_part_latency = 1.5 * src_dst_latency
            first_part_set = []
            second_part_set = []
            # self.log.debug("third sw: %s" % str(third))
            try:
                while first_part_latency + second_part_latency > 2 * src_dst_latency:
                    if len(no_try) == 0:
                        return []
                    third = self.rng.choice(no_try)
                    no_try.remove(third)
                    first_part_set = [p for p in topo.get_shortest_paths(flow.src, third, False)]
                    second_part_set = [p for p in topo.get_shortest_paths(third, flow.dst, False)]
                    first_part_latency = self.calculate_latency(first_part_set[0])
                    second_part_latency = self.calculate_latency(second_part_set[0])
            except nx.exception.NetworkXNoPath:
                # self.log.debug("No shortest path for third %d of (%d, %d)" % (third, flow.src, flow.dst))
                count += 1
                continue

            for f_seg in first_part_set:
                # self.log.debug("1st segment: %s" % f_seg)
                if not self.check_capacity(topo, f_seg, link_caps, flow.vol, flow.reversed_vol):
                    continue
                else:
                    self.allocate_link_cap(f_seg, link_caps, flow.vol, flow.reversed_vol)
                f_seg_except_third = f_seg[0:len(f_seg) - 1]
                for s_seg in second_part_set:
                    if not self.check_capacity(topo, s_seg, link_caps, flow.vol, flow.reversed_vol):
                        continue
                    else:
                        self.allocate_link_cap(s_seg, link_caps, flow.vol, flow.reversed_vol)
                    cycle = False
                    for v in s_seg:
                        if v in f_seg_except_third:
                            cycle = True
                            break
                    if not cycle:
                        path.extend(f_seg_except_third)
                        path.extend(s_seg)
                        if self.old_path is not None:
                            diff = [s for s in path if s not in self.old_path]
                            if not diff:
                                path = []
                                # self.rollback_link_cap(s_seg, link_caps, flow.vol, flow.reversed_vol)
                        break
                    # self.rollback_link_cap(s_seg, link_caps, flow.vol, flow.reversed_vol)
                if path:
                    break
                self.rollback_link_cap(f_seg, link_caps, flow.vol, flow.reversed_vol)
            count += 1

        if count == self.attempts - 3:
            self.log.debug("Fail hard")
            return []

        # flow.path = path
        return path

def get_to_try(topo, flow):
    tried = [flow.src, flow.dst]
    for mdx in flow.mdbxes:
        tried.append(mdx)
    tries = [sw for sw in topo.edge_switches() if sw not in tried]
    return tries


class MultipleMiddleSwitchPathGenerator(PathGenerator):

    def get_node_with_distance(self, bfs, cur, cur_dis, distance, path, output, reversed):
        epath = copy(path)
        if not reversed:
            epath.append(cur)
        else:
            epath.appendleft(cur)
        if cur_dis == distance:
            output[cur] = list(epath)
        else:
            if not bfs.has_key(cur):
                return
            for child in bfs[cur]:
                self.get_node_with_distance(bfs, child, cur_dis + 1, distance, epath, output, reversed)

    def create_with_middlebox(self, topo, flow, from_srcs, to_dsts, link_caps):
        mdbx = flow.mdbxes[0]
        for src_key in from_srcs:
            path_from_src = from_srcs[src_key]
            if src_key in flow.skip_mdbxes or\
                    not self.check_capacity(topo, path_from_src, link_caps, flow.vol, flow.reversed_vol):
                continue
            for dst_key in to_dsts:
                path_to_dst = to_dsts[dst_key]
                if src_key in flow.skip_mdbxes or\
                        self.checking_joint(path_from_src, path_to_dst):
                    continue
                else:
                    if not self.check_capacity(topo, path_to_dst, link_caps, flow.vol, flow.reversed_vol):
                        continue
                    else:
                        # self.log.info("path_from_src: %s" % path_from_src)
                        # self.log.info("path_to_dst: %s" % path_to_dst)
                        in_path = path_from_src + path_to_dst
                        half_from_src_s = [p for p in topo.get_shortest_paths(src_key, mdbx, False)]
                        half_from_dst_s = [p for p in topo.get_shortest_paths(mdbx, dst_key, False)]
                        # self.log.info("half_from_src_s: %s" % half_from_src_s)
                        # self.log.info("half_from_dst_s: %s" % half_from_dst_s)
                        for h_from_src in half_from_src_s:
                            if self.checking_joint(h_from_src[1:len(h_from_src)], in_path):
                                continue

                            for h_to_dst in half_from_dst_s:
                                iin_path = copy(in_path + h_from_src[1:len(h_from_src)-1])
                                if self.checking_joint(h_to_dst[0:len(h_to_dst)-1], iin_path):
                                    continue
                                else:
                                    mid_path = h_from_src[1:len(h_from_src)-1] + h_to_dst[0:len(h_to_dst)-1]
                                    path = path_from_src + mid_path + path_to_dst
                                    if not self.check_capacity(topo, path, link_caps, flow.vol, flow.reversed_vol):
                                        continue
                                    flow.path = path
                                    flow.skip_mdbxes = [src_key, dst_key]
                                    self.log.info(flow.path)
                                    self.allocate_link_cap(flow.path, link_caps, flow.vol, flow.reversed_vol)
                                    return True
        return False

    def create_with_shortest_path(self, topo, flow, from_srcs, to_dsts, link_caps):
        for src_key in from_srcs:
            path_from_src = from_srcs[src_key]
            if not self.check_capacity(topo, path_from_src, link_caps, flow.vol, flow.reversed_vol):
                continue
            for dst_key in to_dsts:
                path_to_dst = to_dsts[dst_key]
                if self.checking_joint(path_from_src, path_to_dst):
                    continue
                else:
                    if not self.check_capacity(topo, path_to_dst, link_caps, flow.vol, flow.reversed_vol):
                        continue
                    try:
                        in_path = path_from_src + path_to_dst
                        mid_paths = [p for p in topo.get_shortest_paths(src_key, dst_key, False)]
                        for mid_path in mid_paths:
                            if mid_path != [] and not self.checking_joint(mid_path, in_path):
                                path = path_from_src + mid_path + path_to_dst
                                if not self.check_capacity(topo, path, link_caps, flow.vol, flow.reversed_vol):
                                    continue
                                flow.path = path
                                flow.skip_mdbxes = [src_key, dst_key]
                                self.log.info(flow.path)
                                self.allocate_link_cap(flow.path, link_caps, flow.vol, flow.reversed_vol)
                                return True
                    except nx.exception.NetworkXNoPath:
                        continue
        return False


    def generate_path(self, topo, flow, link_caps):
        path = []
        count = 0
        self.log.debug("Flow%d: %d --> %d: %s" % (flow.flow_id, flow.src, flow.dst, str(flow.vol)))
        while len(path) == 0 and count < self.attempts - 3:
            try:
                from_srcs = defaultdict()
                bfs_src = nx.bfs_successors(topo.graph, flow.src)
                self.get_node_with_distance(bfs_src, flow.src, 0, 2, deque([]), from_srcs, False)

                to_dsts = defaultdict()
                bfs_dst = nx.bfs_successors(topo.graph, flow.dst)
                self.get_node_with_distance(bfs_dst, flow.dst, 0, 2, deque([]), to_dsts, True)
            except nx.exception.NetworkXNoPath:
                count += 1
                continue

            if not self.create_with_middlebox(topo, flow, from_srcs, to_dsts, link_caps):
                if not self.create_with_shortest_path(topo, flow, from_srcs, to_dsts, link_caps):
                    count += 1
                    continue
            return True
        if count == self.attempts - 3:
            self.log.debug("Fail hard")
            return False
        return path

class RandomWalkPathGenerator(PathGenerator):
    MAX_ITER = 100

    def __init__(self, rng, attempts=10, old_path=None):
        super(RandomWalkPathGenerator, self).__init__()
        self.rng = rng
        self.attempts = attempts
        self.old_path = old_path

    def rnd_walk(self, topo, node, steps, visited = {}):
        i = 0
        visited[node] = True
        # self.log.debug("rnd_walk %d %d" % (node, steps))
        res = [node]
        if steps == 0:
            return res
        while i < self.MAX_ITER:
            i += 1
            nnode = self.rng.choice(topo.graph.neighbors(node))
            if nnode in visited:
                continue
            res.extend(self.rnd_walk(topo, nnode, steps - 1, visited))
            return res
        self.log.debug("Giving up on random walk")
        raise nx.NetworkXNoPath("No random path from %s." % (node))


    def generate_path(self, topo, flow, link_caps):
        path = []
        middle = []
        if len(flow.mdbxes) > 0:
            middle = flow.mdbxes
            middle.insert(0, flow.src)
            middle.append(flow.dst)
        count = 0
        self.log.debug("Flow%d: %d --> %d: %s" % (flow.flow_id, flow.src, flow.dst, str(flow.vol)))
        while len(path) == 0 and count < self.attempts - 3:
            try:
                if len(flow.mdbxes) == 0:
                    middle = []
                    cur = flow.src
                    visited = {flow.src: True, flow.dst: True}
                    while len(middle) < 3:
                        seq = self.rnd_walk(topo, cur, self.rng.choice(range(2,4)), visited)
                        # print seq
                        cur = seq[-1]
                        middle.append(cur)
                    middle.insert(0, flow.src)
                    middle.append(flow.dst)

                # self.log.debug("middle: %s" % middle)

                path_from_src = [flow.src]
                seg_found = False
                for src_key, dst_key in itertools.izip(middle[0:len(middle)-1], middle[1:len(middle)]):
                    # print src_key, dst_key
                    avoid = set(middle).difference(set((src_key, dst_key)))
                    tmp, mid_segs = k_shortest_paths(topo.graph, src_key, dst_key, k=5)
                    # print mid_segs
                    mid_segs = [p for p in mid_segs if len(p) > 2 and not self.checking_joint(p, avoid)]
                    # mid_segs = [p for p in topo.get_shortest_paths(src_key, dst_key, False)]
                    self.rng.shuffle(mid_segs)
                    # print mid_segs
                    seg_found = False
                    for seg in mid_segs:
                        # self.log.debug("path_from_src: %s" % path_from_src)
                        # self.log.debug("seg: %s" % seg)
                        if not self.check_capacity(topo, seg, link_caps, flow.vol, flow.reversed_vol):
                            self.log.debug("check_capacity fail")
                            continue
                        if self.checking_joint(path_from_src, seg[1:len(seg)]):
                            self.log.debug("check_joint fail")
                            continue
                        path_from_src.extend(seg[1:len(seg)])
                        seg_found = True
                        break
                    if seg_found is False:
                        break
                if seg_found is False:
                    count += 1
                    continue

                path = path_from_src
                self.log.debug("path: %s" % path)
            except nx.exception.NetworkXNoPath:
                count += 1
                continue


        if count == self.attempts - 3:
            self.log.debug("Fail hard")
            return False
        flow.path = path
        flow.mdbxes = middle[1:len(middle)-1]
        self.allocate_link_cap(flow.path, link_caps, flow.vol, flow.reversed_vol)
        return True

class HotSpotPathGenerator(PathGenerator):
    pass
