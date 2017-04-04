from domain.network_premitives import *
from flow_change_generator import FlowChangeGenerator
from mul_flow_change_generator import MulFlowChangeGenerator
from path_generators import HotSpotPathGenerator
from path_generators import RandomWalkPathGenerator
from path_generators import ThirdSwitchPathGenerator

class RandomChangeGenerator(MulFlowChangeGenerator):
    def __init__(self, rng, path_gen):
        # third_sw_gen = MultipleMiddleSwitchPathGenerator(rng)
        path_generator = None
        if path_gen == constants.THIRD_SWITCH_GENERATION:
            path_generator = ThirdSwitchPathGenerator(rng)
        elif path_gen == constants.RANDOM_WALK_GENERATION:
            path_generator = RandomWalkPathGenerator(rng)
        elif path_gen == constants.HOT_SPOT_GENERATION:
            path_generator = HotSpotPathGenerator(rng)
        super(RandomChangeGenerator, self).__init__(path_generator, rng)
    #
    # def generate_flows(self, topo, tm, flow_cnt):
    #     debug = self.log.debug
    #     src_dst_gen = self.random_src_dst_gen(topo.edge_switches())
    #     old_link_caps = defaultdict()
    #     new_link_caps = defaultdict()
    #     old_flows = defaultdict()
    #     new_flows = defaultdict()
    #
    #     while len(old_flows) < flow_cnt:
    #         src, dst = next(src_dst_gen)
    #         while old_flows.has_key((src, dst)) or tm[src][dst] == 0\
    #                 or not self.path_generator.check_eligible_src_dst(topo, old_link_caps, src, dst,
    #                                                                   tm[src][dst], tm[dst][src]):
    #             src, dst = next(src_dst_gen)
    #         old_flow = Flow(len(old_flows), src, dst, tm[src][dst])
    #         self.path_generator.attempts = len(topo.edge_switches())
    #         has_old_path = self.path_generator.generate_path(topo, old_flow, old_link_caps)
    #         if not has_old_path:
    #             continue
    #         new_flow = Flow(len(new_flows), src, dst, self.compute_new_vol(old_flow.vol))
    #         # self.third_sw = third
    #         self.old_path = old_flow.path
    #         has_new_path = self.path_generator.generate_path(topo, new_flow, new_link_caps)
    #         if not has_new_path:
    #             self.path_generator.rollback_link_cap(old_flow.path, old_link_caps, old_flow.vol, old_flow.reversed_vol)
    #             continue
    #         new_flows[(src, dst)] = new_flow
    #         old_flows[(src, dst)] = old_flow
    #         debug((old_flow, new_flow))
    #
    #     return self.return_flows(old_flows, new_flows, old_link_caps, new_link_caps)
