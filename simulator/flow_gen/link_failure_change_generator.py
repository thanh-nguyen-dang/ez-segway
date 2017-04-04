import itertools
from copy import deepcopy
from collections import defaultdict
from domain.network_premitives import GenSingleFlow
from flow_change_generator import FlowChangeGenerator
from path_generators import ThirdSwitchPathGenerator, ShortestPathGenerator


class LinkFailureChangeGenerator(FlowChangeGenerator):
    def __init__(self, rng, failure_rate):
        super(LinkFailureChangeGenerator, self).__init__(ThirdSwitchPathGenerator(rng), rng)
        self.failure_rate = failure_rate

    @staticmethod
    def is_affected_link(failed_ids, flows_by_link, endpoints):
        for (src, dst) in failed_ids:
            if endpoints in flows_by_link[(src, dst)]:
                return True
        return False

    def create_old_flows(self, topo, tm, flow_cnt, third_sws, flows_by_link):
        src_dst_gen = self.random_src_dst_gen(topo.edge_switches())
        old_link_caps = defaultdict()
        old_flows = defaultdict()
        total_pairs = (len(topo.edge_switches()) * (len(topo.edge_switches()) - 1))

        failed_pairs = defaultdict()
        while len(old_flows) < flow_cnt and len(failed_pairs.keys()) + len(old_flows.keys()) < total_pairs:
            src, dst = next(src_dst_gen)
            vol = tm[src][dst] + tm[dst][src]
            possible_pair = self.path_generator.check_eligible_src_dst(topo, old_link_caps, src, dst,
                                                                       tm[src][dst], tm[dst][src])
            while (old_flows.has_key((src, dst)) or failed_pairs.has_key((src, dst))
                        or tm[src][dst] == 0 or not possible_pair)\
                    and (len(failed_pairs.keys()) + len(old_flows.keys()) < total_pairs):
                if not possible_pair:
                    failed_pairs[(src, dst)] = True
                src, dst = next(src_dst_gen)
                possible_pair = self.path_generator.check_eligible_src_dst(topo, old_link_caps, src, dst,
                                                                           tm[src][dst], tm[dst][src])

            if len(failed_pairs.keys()) + len(old_flows.keys()) >= total_pairs:
                break

            old_flow = GenSingleFlow(len(old_flows), src, dst, vol)
            self.path_generator.attempts = len(topo.edge_switches())
            has_old_path, third = self.path_generator.generate_path_with_third_switch(topo, old_flow, old_link_caps)
            if not has_old_path:
                failed_pairs[(src, dst)] = True
                continue
            else:
                third_sws[(src, dst)] = third
            for i, j in itertools.izip(old_flow.path[0:len(old_flow.path) - 1],
                                           old_flow.path[1:len(old_flow.path)]):
                flows_by_link[(i, j)].add((src, dst))
            self.log.debug(old_flow)
            old_flows[(src, dst)] = old_flow

        if len(failed_pairs.keys()) + len(old_flows.keys()) >= total_pairs:
            self.log.debug("Has %d flows" % len(old_flows.keys()))
        return old_flows, old_link_caps

    def generate_failed_edges_by_percentage(self, flows_by_link, failure_rate, flow_cnt):
        chosen_keys = []
        affected_flows = set()
        not_chosen_keys = deepcopy(flows_by_link.keys())
        while len(affected_flows) < flow_cnt * failure_rate:
            if len(not_chosen_keys) == 0:
                return chosen_keys
            chosen_key = self.rng.choice(not_chosen_keys)
            not_chosen_keys.remove(chosen_key)
            affected_flows = affected_flows | flows_by_link[chosen_key]
            chosen_keys.append(chosen_key)

        return chosen_keys

    def generate_flows(self, topo, old_tm, flow_cnt):
        debug = self.log.debug
        third_sws = defaultdict()
        flows_by_link = defaultdict(set)

        old_flows, old_link_caps = self.create_old_flows(topo, old_tm, flow_cnt, third_sws, flows_by_link)
        if len(flows_by_link.keys()) == 0:
            return [], []

        failed_ids = self.generate_failed_edges_by_percentage(flows_by_link, self.failure_rate, flow_cnt)

        new_topo = deepcopy(topo)
        for i, j in failed_ids:
            new_topo.graph.remove_edge(i, j)
        new_link_caps = defaultdict()
        new_flows = defaultdict()
        changed_paths = defaultdict()
        bad_pairs = defaultdict()

        for (src, dst) in old_flows.keys():
            old_flow = old_flows[(src, dst)]

            new_flow = GenSingleFlow(old_flow.flow_id, src, dst, self.compute_new_vol(old_flow.vol))
            if not self.is_affected_link(failed_ids, flows_by_link, (src, dst)):
                new_flow.path = old_flow.path
                if self.path_generator.check_capacity(new_topo, new_flow.path, new_link_caps, new_flow.vol):
                    self.path_generator.allocate_link_cap(new_flow.path, new_link_caps,
                                                          new_flow.vol, new_flow.reversed_vol)
                else:
                    self.set_back_to_old_flow(new_flow, old_flow, new_link_caps)
            else:
                changed = self.try_generate_new_path(new_topo, new_flow, old_flow, new_link_caps, third_sws)
                if changed:
                    changed_paths[(src, dst)] = True
                else:
                    bad_pairs[(src, dst)] = True
            new_flows[(src, dst)] = new_flow

        if len(changed_paths) < self.failure_rate * flow_cnt:
            for (src, dst) in old_flows.keys():
                if (src, dst) not in changed_paths.keys() and (src, dst) not in bad_pairs.keys():
                    new_flow = new_flows[(src, dst)]
                    old_flow = old_flows[(src, dst)]
                    changed = self.try_generate_new_path(new_topo, new_flow, old_flow, new_link_caps,\
                                                     third_sws)
                    if changed:
                        changed_paths[(src, dst)] = True
                if len(changed_paths) == self.failure_rate * flow_cnt:
                    break

        return self.return_flows(old_flows, new_flows, old_link_caps, new_link_caps)

    def try_generate_new_path(self, new_topo, new_flow, old_flow, new_link_caps, third_sws):
        changed = True
        self.path_generator.attempts = len(new_topo.edge_switches())
        self.path_generator.third_sw = third_sws[(new_flow.src, new_flow.dst)]
        self.path_generator.old_path = old_flow.path
        has_new_path, third = self.path_generator.generate_path(new_topo, new_flow, new_link_caps)
        if not has_new_path:
            shortest_path_generator = ShortestPathGenerator()
            if not shortest_path_generator.generate_path(new_topo, new_flow, new_link_caps):
                self.set_back_to_old_flow(new_flow, old_flow, new_link_caps)
                changed = False
                self.log.debug("cannot make new flow")
        return changed
