from domain.network_premitives import *
from collections import defaultdict
from domain.execution_info import ExecutionInfo


class EzScheduler(object):
    def __init__(self, id_, log_):
        self.id = id_
        self.log = log_
        self.reset()

    def reset(self):
        self.links_by_endpoints = defaultdict(Link)
        self.segments_by_seg_path_id = defaultdict(LinkSegment)
        self.segments_to_be_done = set()
        self.suspecting_deadlocks = {}
        self.in_deadlocks = set([])
        self.splitting_links = set([])
        self.segments_resolving_deadlock = set([])
        self.scheduling_mode = constants.NORMAL_MODE
        self.can_violate_congestion = False
        self.trace = ExecutionInfo()
        self.suspecting_deadlock_for_this_test = False

    # def find_dependency_loop_for_link(self, link, links_by_endpoint, segments_by_seg_path_id):
    #     for u_op in link.to_adds:
    #         assert isinstance(u_op, UpdateOperation)
    #         stack = [u_op]
    #         # debug("start checking loop link %d->%d, update_op %s"\
    #         #       % (link.src, link.dst, u_op.seg_path_id))
    #         count_op = 0
    #         count_link = 0
    #         count_dependency_loop = 0
    #         while len(stack) > 0:
    #             curr_item = stack.pop()
    #             if isinstance(curr_item, UpdateOperation):
    #                 (c, c_l) = self.traverse_op(stack, \
    #                                             segments_by_seg_path_id[curr_item.seg_path_id], \
    #                                             links_by_endpoint, link)
    #                 count_op += c
    #                 count_dependency_loop += c_l
    #                 # debug("total count=%d, total count_loop=%d" % (count_op, count_dependency_loop))
    #             elif isinstance(curr_item, Link):
    #                 count_link = self.traverse_link(stack, curr_item)
    #         # debug("end checking loop link %d->%d, update_op %s"\
    #         #       % (link.src, link.dst, u_op.seg_path_id))
    #         # debug("final count=%d, final count_loop=%d" % (count_op, count_dependency_loop))
    #         u_op.nb_ops = count_op
    #         u_op.nb_links = count_link
    #         u_op.nb_dependency_loop = count_dependency_loop
    #         if u_op.nb_dependency_loop > 0:
    #             link.to_adds_loop.append(u_op)
    #
    #         link.to_adds[:] = [x for x in link.to_adds if x.nb_dependency_loop == 0]
    #
    #     link.to_adds = list(reversed(sorted(link.to_adds)))
    #     link.to_adds_loop = list(reversed(sorted(link.to_adds_loop)))
    #
    # def traverse_link(self, stack, link):
    #     count = 0
    #     for u_op in link.to_adds:
    #         stack.append(u_op)
    #         count += 1
    #     return count
    #
    # def traverse_op(self, stack, l_segment, links_by_endpoint, origin_link):
    #     count = 0
    #     count_loop = 0
    #     for pair in l_segment.old_link_seg:
    #         # self.log.debug("traverse link %s" % pair)
    #         curr_link = links_by_endpoint[pair]
    #         if curr_link == origin_link:
    #             count_loop += 1
    #         if curr_link.avail_cap < l_segment.vol: # == 0:  # it should be same with the avail_cap < vol
    #             # self.log.debug("add %d->%d" % curr_link.src, curr_link.dst)
    #             stack.append(curr_link)
    #             count += 1
    #     # self.log.debug("count=%d, count_loop=%d" % (count, count_loop))
    #     return count, count_loop
