from copy import deepcopy
from domain.network_premitives import Link
from domain.message import NotificationMessage
from ez_lib.ez_ob import P2PUpdateInfo, UpdateNext, UpdateOperation
from ez_lib import ez_flow_tool
from time import time
from collections import deque, defaultdict
from misc import constants
import eventlet

from ez_scheduler import EzScheduler

class P2PScheduler(EzScheduler):
    good_to_move_succs = set()
    coherent_succs = set()
    removing_preds = set()
    new_pred_by_seg_path_id = {}
    new_succ_by_seg_path_id = {}
    old_pred_by_seg_path_id = {}
    old_succ_by_seg_path_id = {}
    remaining_vol_of_dependency_loop = {}
    remaining_vol_of_normal_op = {}
    remaining_vol_for_resolvers = {}
    has_execution = False
    compute_critical_cycle = True
    # suspecting_deadlocks = {}
    # in_deadlocks = set([])
    # scheduling_mode = constants.NORMAL_MODE
    # can_violate_congestion = False
    # trace = ExecutionInfo()

    def __init__(self, id_, neighbor_ids, log_, callback_for_new_update):
        super(P2PScheduler, self).__init__(id_, log_)
        self.neighbor_ids = neighbor_ids
        self.current_update = -1
        self.callback_for_new_update = callback_for_new_update
        self.reset()

    def reset(self):
        super(P2PScheduler, self).reset()
        self.remaining_good_vol_to_move = {}
        self.good_to_move_succs = set()
        self.coherent_succs = set()
        self.removing_preds = set()
        self.new_pred_by_seg_path_id = {}
        self.new_succ_by_seg_path_id = {}
        self.old_pred_by_seg_path_id = {}
        self.old_succ_by_seg_path_id = {}
        self.remaining_vol_of_dependency_loop = {}
        self.remaining_vol_of_normal_op = {}
        self.remaining_vol_for_resolvers = {}
        self.has_execution = False

    def compute_required_vol_for_dependency_loop(self):
        for link in self.links_by_endpoints.values():
            self.remaining_vol_of_dependency_loop[(link.src, link.dst)] = 0
            self.remaining_vol_of_normal_op[(link.src, link.dst)] = 0
            adding_vol = 0
            for add_op in link.to_adds_loop:
                self.remaining_vol_of_dependency_loop[(link.src, link.dst)] \
                    += self.segments_by_seg_path_id[add_op.seg_path_id].vol
                adding_vol += self.segments_by_seg_path_id[add_op.seg_path_id].vol

            for add_op in link.to_adds:
                self.remaining_vol_of_normal_op[(link.src, link.dst)] \
                    += self.segments_by_seg_path_id[add_op.seg_path_id].vol
                adding_vol += self.segments_by_seg_path_id[add_op.seg_path_id].vol

            for add_op in link.to_adds_only:
                adding_vol += self.segments_by_seg_path_id[add_op.seg_path_id].vol

            link.necessary_additional_cap = adding_vol - link.avail_cap
            link.unnecessary_additional_cap = adding_vol - link.necessary_additional_cap
            # self.log.debug("remaining required vol of dependency loop: %s" % str(self.remaining_vol_of_dependency_loop))

    def create_update_next(self, update_type, seg_path_id):
        l_segment = self.segments_by_seg_path_id[seg_path_id]
        (flow_id, segment_id) = seg_path_id
        if update_type == constants.ADD_NEXT or update_type == constants.UPDATE_NEXT:
            return UpdateNext(seg_path_id, self.new_succ_by_seg_path_id[seg_path_id], update_type)
        elif update_type == constants.REMOVE_NEXT:
            return UpdateNext(seg_path_id, self.old_succ_by_seg_path_id[seg_path_id], update_type)
        elif update_type == constants.NO_UPDATE_NEXT:
            return UpdateNext(seg_path_id, None, update_type)

    def create_new_msg_to_sw_with_id(self, dst_id, msg_type, seg_path_id, split_vol, send_to_other_segment=False):
        if msg_type != constants.COHERENT_MSG:
            new_msg = NotificationMessage(self.id, dst_id, msg_type, seg_path_id, self.current_update, time() * 1000)
            if not send_to_other_segment:
                l_segment = self.segments_by_seg_path_id[seg_path_id]
                if l_segment.vol == 0: # Spliting means that after executing, there are some volume left for l_segment
                    new_msg.split_vol = split_vol
        else:
            new_msg = NotificationMessage(self.id, dst_id, msg_type, seg_path_id, self.current_update, time() * 1000)
        return new_msg

    def append_update_infos(self, update_infos, (new_msgs, update_next)):
        self.update_message_list(update_infos, new_msgs)
        self.update_one_update_next(update_infos, update_next)

    def extend_update_infos(self, update_infos, (new_msgs, new_update_nexts)):
        self.update_message_list(update_infos, new_msgs)
        for update_next in new_update_nexts:
            self.update_one_update_next(update_infos, update_next)

    def update_message_list(self, update_infos, new_msgs):
        for msg in new_msgs:
            if msg.msg_type == constants.COHERENT_MSG:
                flow_id = msg.seg_path_id
            else:
                (flow_id, segment_id) = msg.seg_path_id

            if update_infos.has_key(flow_id):
                update_infos[flow_id].msgs.append(msg)
            elif msg.msg_type != constants.COHERENT_MSG:
                if msg.seg_path_id in self.segments_to_be_done:
                    l_segment = self.segments_by_seg_path_id[msg.seg_path_id]
                else:
                    (flow_id, segment_id) = msg.seg_path_id
                    segment_id -= 1
                    l_segment = self.segments_by_seg_path_id[(flow_id, segment_id)]
                update_infos[flow_id] = P2PUpdateInfo(flow_id, l_segment.flow_src, l_segment.flow_dst, [msg])

    def update_one_update_next(self, update_infos, update_next):
        if update_next is not None:
            (flow_id, segment_id) = update_next.seg_path_id
            if update_infos.has_key(flow_id):
                update_infos[flow_id].update_next_sw = update_next
            else:
                l_segment = self.segments_by_seg_path_id[update_next.seg_path_id]
                update_infos[flow_id] = P2PUpdateInfo(flow_id, l_segment.flow_src, l_segment.flow_dst, [], update_next)

    # Input: seg_path_id of an update operation and the volume of executable path
    # Doing::
    # - Update capacity for link, volume for remaining part of update operation
    # - Update relation from link to link (released/received capacity
    # from an update operation
    def execute_by_seg_path_id(self, seg_path_id, consumed_vol):
        self.has_execution = True
        # self.log.info("sw%d: execute %s (%s)" % (self.id, seg_path_id, consumed_vol))
        l_segment = self.segments_by_seg_path_id[seg_path_id]
        for link_id in l_segment.new_link_seg:
            link = self.links_by_endpoints[link_id]
            # if link.scheduling_mode == constants.SUSPECTING_LINK:
            #     self.unsuspecting_link(link)
            link.avail_cap -= consumed_vol
            self.update_remaining_capacity_for_loop_or_non_loop_update(link, l_segment.seg_path_id, consumed_vol)
            # call the line above before calling to update_released_cap_in_link_to_link_relations, and
            # update_received_cap_in_link_to_link_relations

            if (self.scheduling_mode & constants.CONGESTION_MODE) != constants.CONGESTION_MODE:
                assert link.avail_cap >= 0, "link %s has available capacity smaller than 0: %s" % (link, link.avail_cap)
            self.update_released_cap_in_link_to_link_relations(link)
            # self.log.debug(self.links_by_endpoints[link_id])
            # self.log.info("link %s->%s has %s remaining" % (link.src, link.dst, link.avail_cap))

        self.segments_by_seg_path_id[seg_path_id].vol -= consumed_vol
        for link_id in l_segment.old_link_seg:
            if self.links_by_endpoints.has_key(link_id):
                old_link = self.links_by_endpoints[link_id]
                old_link.avail_cap += consumed_vol
                if self.segments_by_seg_path_id[seg_path_id].vol == 0:
                    old_link.to_removes[:] = \
                        [x for x in old_link.to_removes if x.seg_path_id == seg_path_id]
                self.update_received_cap_in_link_to_link_relations(old_link)

        self.update_splitting_status(self.segments_by_seg_path_id[seg_path_id])
        if self.id == l_segment.init_sw and not l_segment.remove_only:
            self.trace.time_using_new_path_by_seg_path_id[l_segment.seg_path_id] = time() * 1000
        elif not l_segment.remove_only:
            self.trace.time_new_next_sw_by_seg_path_id[l_segment.seg_path_id] = \
                (time() * 1000, self.new_succ_by_seg_path_id[l_segment.seg_path_id], constants.ADD_NEXT)
# self.log.info("sw%d: after executing %s (%s) remaining" % (self.id, seg_path_id,
#                                                            self.segments_by_seg_path_id[seg_path_id].vol))

    def execute_operation_on_link(self, link, l_segment, finished_list, affected_links, consume_vol):
        seg_path_id = l_segment.seg_path_id
        new_succ = self.new_succ_by_seg_path_id[seg_path_id] if self.new_succ_by_seg_path_id.has_key(
                seg_path_id) else None
        new_msgs = []
        update_next = None
        if (l_segment.loop_pos == constants.OLD_IN_SEGMENT_LOOP_POS and self.id == l_segment.init_sw
            and self.check_the_whole_segment_is_good_to_move(seg_path_id)):
            new_msgs, update_next = self.execute_old_in_loop_seg_end_sw(seg_path_id, consume_vol)
            if l_segment.vol == 0:
                finished_list.add(seg_path_id)
            old_next_id = self.old_succ_by_seg_path_id[seg_path_id]
            self.update_affected_links(affected_links, old_next_id)
        elif (self.is_switch_end_sw_of_not_new_path_in_loop_segment(new_succ, l_segment)
              or seg_path_id in self.good_to_move_succs):
            self.execute_by_seg_path_id(seg_path_id, consume_vol)
            return self.create_update_infos(seg_path_id, finished_list, affected_links, consume_vol)
        return new_msgs, update_next

    def update_splitting_status(self, l_segment):
        if l_segment.vol > 0: # it means that the segment is being split, still being split now
            if not l_segment.is_splitting: # START SPLITTING
                if l_segment.type_of_update == 2:
                    self.trace.no_of_working_rules += 2
                elif l_segment.type_of_update == 3:
                    self.trace.no_of_working_rules += 1

                l_segment.is_splitting = True
                self.scheduling_mode |= constants.SPLITTING_MODE

                self.trace.no_segments_splitting += 1
                if self.trace.no_segments_splitting > self.trace.max_splitting_segments:
                    self.trace.max_splitting_segments = self.trace.no_segments_splitting
                self.trace.no_of_splitting_by_time.append((time(), self.trace.no_segments_splitting))
                self.trace.update_overhead_info()
        elif l_segment.is_splitting: # it means that, the segment was split and now stop splitting
            self.trace.no_segments_splitting -= 1
            self.trace.no_of_splitting_by_time.append((time(), self.trace.no_segments_splitting))
            if l_segment.type_of_update == 2:
                self.trace.no_of_working_rules -= 2
            elif l_segment.type_of_update == 3:
                self.trace.no_of_working_rules -= 1
            l_segment.is_splitting = False
        else: # normal update, no split
            if l_segment.type_of_update == 2:
                self.trace.no_of_working_rules += 1
            if l_segment.type_of_update == 1:
                self.trace.no_of_working_rules -= 1

        if self.trace.max_no_of_working_rules < self.trace.no_of_working_rules:
            self.trace.max_no_of_working_rules = self.trace.no_of_working_rules

    # Input: link capacity
    # Doting: Change status of a link from suspecting to no_suspecting
    def unsuspecting_link(self, link):
        self.suspecting_deadlocks.pop((link.src, link.dst), None)
        link.scheduling_mode = constants.NORMAL_LINK

    def update_remaining_capacity_for_loop_or_non_loop_update(self, link, seg_path_id, consumed_vol):
        is_loop = False
        link_id = (link.src, link.dst)
        for u_op in link.to_adds_loop:
            if u_op.seg_path_id == seg_path_id:
                self.remaining_vol_of_dependency_loop[link_id] -= consumed_vol
                is_loop = True
        if not is_loop:
            for u_op in link.to_adds:
                if u_op.seg_path_id == seg_path_id:
                    self.remaining_vol_of_normal_op[link_id] -= consumed_vol
        if seg_path_id in self.in_deadlocks and self.remaining_vol_for_resolvers.has_key(link_id):
            self.remaining_vol_for_resolvers[link_id] -= consumed_vol

    def remove_deadlock_link(self, link_id):
        self.in_deadlocks.remove(link_id)
        link = self.links_by_endpoints[link_id]
        for u_op in link.to_removes:
            if u_op.seg_path_id in self.segments_resolving_deadlock:
                self.segments_resolving_deadlock.remove(u_op.seg_path_id)

    def add_deadlock_link(self, link_id):
        self.in_deadlocks.add(link_id)
        link = self.links_by_endpoints[link_id]
        for u_op in link.to_removes:
            self.segments_resolving_deadlock.add(u_op.seg_path_id)

    # def exist_receive_but_not_executed(self, link):
    #     for u_op in link.to_adds + link.to_adds_loop:
    #         succ_id = self.new_succ_by_seg_path_id[u_op.seg_path_id]
    #         if u_op.seg_path_id in self.good_to_move_succs \
    #                 or succ_id == self.segments_by_seg_path_id[u_op.seg_path_id].end_sw_new:
    #             return True
    #     return False

    def try_removing_deadlock_or_suspecting_link(self, link):
        link_id = (link.src, link.dst)
        if (link_id in self.in_deadlocks or
                    link.scheduling_mode == constants.SUSPECTING_LINK) and \
                        link.avail_cap >= self.total_pending_cycle_vol(link):
                # not self.exist_receive_but_not_executed(link) and \
            if link.scheduling_mode == constants.SUSPECTING_LINK:
                # self.suspecting_deadlocks.pop(link_id)
                self.unsuspecting_link(link)
            else:
                self.remove_deadlock_link(link_id)
                # self.in_deadlocks.remove(link_id)

    def update_received_cap_in_link_to_link_relations(self, link):
        self.try_removing_deadlock_or_suspecting_link(link)

    def update_released_cap_in_link_to_link_relations(self, link):
        self.try_removing_deadlock_or_suspecting_link(link)

    def update_affected_links(self, affected_links, old_next_id):
        endpoints = (self.id, old_next_id)
        if endpoints not in affected_links:
            affected_links.append(endpoints)

    def create_update_infos(self, seg_path_id, finished_list, affected_links, split_vol):
        update_next = None
        new_msgs = []
        l_segment = self.segments_by_seg_path_id[seg_path_id]
        if self.id != l_segment.init_sw:
            if self.new_pred_by_seg_path_id.has_key(seg_path_id):
                update_next = self.create_update_next(constants.ADD_NEXT, seg_path_id)
                new_msgs.append(self.create_new_msg_to_sw_with_id(self.new_pred_by_seg_path_id[seg_path_id],
                                                            constants.GOOD_TO_MOVE_MSG, seg_path_id, split_vol))
        else:
            if self.old_succ_by_seg_path_id.has_key(seg_path_id):
                old_next_id = self.old_succ_by_seg_path_id[seg_path_id]
                if old_next_id != l_segment.end_sw_old:
                    new_msgs.append(
                            self.create_new_msg_to_sw_with_id(old_next_id,
                                                              constants.REMOVING_MSG, seg_path_id, split_vol))
                self.update_affected_links(affected_links, old_next_id)
                if self.new_succ_by_seg_path_id.has_key(seg_path_id):
                    update_next = self.create_update_next(constants.UPDATE_NEXT, seg_path_id)
                else:
                    update_next = self.create_update_next(constants.REMOVE_NEXT, seg_path_id)
            else:
                update_next = self.create_update_next(constants.ADD_NEXT, seg_path_id)
        if l_segment.vol == 0:
            finished_list.add(seg_path_id)
        return new_msgs, update_next

    # STARTING REGION OF UPDATE SPLIT
    def total_pending_cycle_vol(self, link):
        total_vol = 0
        for add_op in link.to_adds + link.to_adds_loop:
            total_vol += self.segments_by_seg_path_id[add_op.seg_path_id].vol
        return total_vol

    def check_to_split(self, link, l_segment):
        if l_segment.seg_path_id not in self.good_to_move_succs and \
                        l_segment.end_sw_new != self.new_succ_by_seg_path_id[l_segment.seg_path_id]:
            # self.log.info("Fail to split because of there is no msg!")
            return 0
        # self.log.info("Check to split %s; %s; %s." % (l_segment.vol, link.avail_cap, l_segment.is_splitting))
        if l_segment.vol > link.avail_cap > 0 or (l_segment.is_splitting and link.avail_cap > 0):
            split_vol = min(link.avail_cap, l_segment.vol)
            return split_vol
        elif self.is_capable(l_segment.seg_path_id):
            return l_segment.vol
        return 0

    def check_for_non_deadlock_operation(self, l_segment):
        if self.is_capable(l_segment.seg_path_id):
            return l_segment.vol
        return 0

    def check_for_deadlock_operation(self, l_segment, link, msg_split_vol):
        if msg_split_vol is not None:
            return msg_split_vol
        return self.check_to_split(link, l_segment)

    def check_and_execute_update_on_link(self, link, seg_path_id, finished_list, affected_links, msg_split_vol=None):
        if seg_path_id not in self.segments_to_be_done:
            return [], None
        l_segment = self.segments_by_seg_path_id[seg_path_id]

        if (link.src, link.dst) in self.in_deadlocks:
            consumed_vol = self.check_for_deadlock_operation(l_segment, link, msg_split_vol)
        else:
            consumed_vol = self.check_for_non_deadlock_operation(l_segment)

        if consumed_vol > 0:
            return self.execute_operation_on_link(link, l_segment, finished_list, affected_links, consumed_vol)
        return [], None

    def check_to_continue(self, link):
        if (self.scheduling_mode & constants.CONGESTION_MODE) == constants.CONGESTION_MODE:
            return True
        if (link.src, link.dst) in self.in_deadlocks and \
            (self.scheduling_mode & constants.SPLITTING_MODE) == constants.SPLITTING_MODE:
            return True
        return False

    def check_and_execute_all_updates_on_link(self, link, update_infos, affected_links):
        self.log.debug("check all splits on link %d->%d" % (link.src, link.dst))
        finished_op_loops = set([])
        finished_list = set([])
        length_before = len(update_infos)
        self.log.debug("list of received GOOD_TO_MOVE %s" % self.good_to_move_succs)
        if len(link.to_adds_loop) + len(link.to_adds) + len(link.to_adds_only) <= 0:
            self.try_removing_deadlock_or_suspecting_link(link)
            return -1
        link_id = (link.src, link.dst)
        for add_op in link.to_adds_loop:
            self.append_update_infos(update_infos,
                                     self.check_and_execute_update_on_link(link, add_op.seg_path_id,
                                                                           finished_op_loops, affected_links)
                                     )
        if self.check_to_continue(link) or\
                        link.avail_cap >= self.remaining_vol_of_dependency_loop[link_id]:
            resolvers, non_resolvers, self.remaining_vol_for_resolvers[link_id] = \
                self.get_list_segments_in_deadlock_benefited_by_link(link)
            # self.log.info("split - link:%d->%d; resolvers: %s\t non-resolvers: %s\t resolver_vol: %s" %
            #               (link.src, link.dst, resolvers, non_resolvers, self.remaining_vol_for_resolvers[link_id]))
            for add_op in resolvers:
                self.append_update_infos(update_infos, self.check_and_execute_update_on_link(
                                            link, add_op.seg_path_id, finished_list, affected_links))
                if not self.check_to_continue(link) and \
                                link.avail_cap < self.remaining_vol_of_dependency_loop[link_id]:
                    break
            if self.check_to_continue(link) or \
                            link.avail_cap >= self.remaining_vol_of_dependency_loop[link_id] + \
                            self.remaining_vol_for_resolvers[link_id]:
                for add_op in non_resolvers:
                    self.append_update_infos(update_infos, self.check_and_execute_update_on_link(
                                                link, add_op.seg_path_id, finished_list, affected_links))
                    if not self.check_to_continue(link) and \
                                    link.avail_cap < (self.remaining_vol_of_dependency_loop[(link.src, link.dst)] +
                                                          self.remaining_vol_for_resolvers[link_id]):
                        break
        if self.check_to_continue(link) or\
                        link.avail_cap >= self.remaining_vol_of_dependency_loop[link_id] \
                        + self.remaining_vol_of_normal_op[link_id]:
            for add_op in link.to_adds_only:
                self.append_update_infos(update_infos,
                                         self.check_and_execute_update_on_link(
                                             link, add_op.seg_path_id, finished_list, affected_links)
                                         )
                if not self.check_to_continue(link) and link.avail_cap < \
                        (self.remaining_vol_of_dependency_loop[link_id]
                             + self.remaining_vol_of_normal_op[link_id]):
                    break
        self.remove_finished_ops(link, finished_op_loops, finished_list)
        self.update_splitting_mode_to_link(link)
        self.remaining_vol_for_resolvers.pop(link_id, None)
        length_after = len(update_infos)
        return length_after - length_before
    # END REGION OF UPDATE SPLIT

    def update_splitting_mode_to_link(self, link):
        is_splitting = False
        for up_op in link.to_adds + link.to_adds_loop:
            l_segment = self.segments_by_seg_path_id[up_op.seg_path_id]
            if l_segment.is_splitting:
                is_splitting = True
                break
        if is_splitting:
            link.scheduling_mode = constants.SPLITTING_LINK
        else:
            link.scheduling_mode = constants.NORMAL_LINK

    def execute_old_in_loop_seg_end_sw(self, seg_path_id, executed_vol):
        new_msgs = []
        update_next = None
        (path, old_in_loop_seg) = seg_path_id
        succ_l_segment_id = (path, old_in_loop_seg + 1)
        self.execute_by_seg_path_id(seg_path_id, executed_vol)
        # self.execute_by_seg_path_id(succ_l_segment_id)
        new_msgs.append(
            self.create_new_msg_to_sw_with_id(self.new_pred_by_seg_path_id[succ_l_segment_id],
                                              constants.GOOD_TO_MOVE_MSG, succ_l_segment_id, executed_vol,
                                              send_to_other_segment=True))
        new_msgs.append(
            self.create_new_msg_to_sw_with_id(self.old_succ_by_seg_path_id[seg_path_id],
                                              constants.REMOVING_MSG, seg_path_id, executed_vol))
        update_next = self.create_update_next(constants.UPDATE_NEXT, seg_path_id)
        return new_msgs, update_next

    def check_the_whole_segment_is_good_to_move(self, seg_path_id):
        return (seg_path_id in self.good_to_move_succs
                or self.segments_by_seg_path_id[seg_path_id].end_sw_new == self.new_succ_by_seg_path_id[seg_path_id])

    def is_switch_end_sw_of_not_new_path_in_loop_segment(self, new_succ, l_segment):
        return (new_succ == l_segment.end_sw_new
                and l_segment.loop_pos != constants.NEW_IN_SEGMENT_LOOP_POS)

    def execute_all_remove_only_updates(self, update_infos, affected_links):
        removed_segment = []
        for l_segment in self.segments_by_seg_path_id.values():
            old_sws = set()
            old_sws.add(l_segment.init_sw)
            for (src, dst) in l_segment.old_link_seg:
                old_sws.add(src)
                old_sws.add(dst)
                if self.id == src:
                    self.update_affected_links(affected_links, dst)

            new_msgs = []
            seg_path_id = l_segment.seg_path_id
            if l_segment.remove_only and self.id in old_sws:
                self.execute_by_seg_path_id(seg_path_id, l_segment.vol)
                if self.id != l_segment.flow_dst:
                    update_next = self.create_update_next(constants.REMOVE_NEXT, seg_path_id)
                    self.append_update_infos(update_infos,
                                             (new_msgs, update_next)
                                             )
                removed_segment.append(l_segment.seg_path_id)
        return removed_segment

    # STARTING REGION OF UPDATE THE WHOLE FLOW
    def is_capable(self, seg_path_id):
        if (self.scheduling_mode & constants.CONGESTION_MODE) == constants.CONGESTION_MODE:
            return True
        l_segment = self.segments_by_seg_path_id[seg_path_id]
        for endpoints in l_segment.new_link_seg:
            link = self.links_by_endpoints[endpoints]
            is_dependency_loop_op = False
            for op in link.to_adds_loop:
                if op.seg_path_id == seg_path_id:
                    is_dependency_loop_op = True
                    break
            is_add_only = False
            for op in link.to_adds_only:
                if op.seg_path_id == seg_path_id:
                    is_add_only = True
                    break

#self.log.info("link %s avail cap %s" % (str(endpoints), link.avail_cap))
            if (not is_dependency_loop_op and (link.avail_cap - l_segment.vol <
                                                   self.remaining_vol_of_dependency_loop[endpoints])) \
                    or (is_dependency_loop_op and link.avail_cap < l_segment.vol) \
                    or (is_add_only and (link.avail_cap - l_segment.vol <
                                                   self.remaining_vol_of_dependency_loop[endpoints]
                                                    + self.remaining_vol_of_normal_op[endpoints])):
#               self.log.info("link %s (with avail cap %s) is not capable for %s (with volume %s)"
#                               % (str(endpoints), link.avail_cap, str(l_segment.seg_path_id), l_segment.vol))
                return False
        return True

    def check_and_execute_all_updates(self, update_infos, affected_links):
        # affected_links = deque([])
        self.has_execution = True
        while self.has_execution:
            self.has_execution = False
            for link in self.links_by_endpoints.values():
                self.check_and_execute_all_updates_on_link(link, update_infos, affected_links)
                for u_op in link.to_adds_loop + link.to_adds:
                    if self.is_switch_end_sw_of_not_new_path_in_loop_segment(link.dst,
                                                                             self.segments_by_seg_path_id[u_op.seg_path_id]):
                        self.suspect_deadlock_on_link(link)

    def get_list_segments_in_deadlock_benefited_by_link(self, link):
        resolver_segs = []
        non_resolver_segs = []
        resolver_vol = 0
        # self.log.info("deadlock links: %s" % self.in_deadlocks)
        # self.log.info("resolving segments: %s" % self.segments_resolving_deadlock)

        for add_op in link.to_adds:
            if add_op.seg_path_id in self.segments_resolving_deadlock:
                resolver_segs.append(add_op)
                resolver_vol += self.segments_by_seg_path_id[add_op.seg_path_id].vol
            else:
                non_resolver_segs.append(add_op)

        return resolver_segs, non_resolver_segs, resolver_vol

    def release_capacity_by_removing_msg(self, msg):
        if msg.seg_path_id not in self.segments_to_be_done:
            return constants.ID_NULL, constants.ID_NULL
        l_segment = self.segments_by_seg_path_id[msg.seg_path_id]
        if l_segment is None:
            return constants.ID_NULL, constants.ID_NULL
        succ_id = self.old_succ_by_seg_path_id[msg.seg_path_id] \
            if self.old_succ_by_seg_path_id.has_key(msg.seg_path_id) \
            else None

        if succ_id is not None:
            release_vol = l_segment.vol if msg.split_vol == -1 else msg.split_vol
            link = self.links_by_endpoints[(self.id, succ_id)]
            self.release_capacity(link, l_segment, release_vol)
            self.update_received_cap_in_link_to_link_relations(link)
            self.update_splitting_status(self.segments_by_seg_path_id[msg.seg_path_id])
            self.trace.time_new_next_sw_by_seg_path_id[l_segment.seg_path_id] = \
                (time() * 1000, succ_id, constants.REMOVE_NEXT)
            return succ_id, l_segment.vol
        else:
            self.log.debug("succ_id is None for %s segment" % str(l_segment.seg_path_id))
            return None, None

    def release_capacity(self, link, l_segment, vol):
        link.avail_cap += vol
        l_segment.vol -= vol
        if l_segment.vol == 0:
            link.to_removes[:] = \
                [x for x in link.to_removes if x.seg_path_id == l_segment.seg_path_id]

        self.log.debug("release capacity: link %d->%d: avail_cap %f" % (link.src, link.dst, link.avail_cap))

    def remove_finished_ops(self, link, finished_op_loops, finished_ops):
        link.to_adds_loop[:] = \
            [x for x in link.to_adds_loop if x.seg_path_id not in finished_op_loops]
        link.to_adds[:] = \
            [x for x in link.to_adds if x.seg_path_id not in finished_ops]
        link.to_adds_only[:] = \
            [x for x in link.to_adds_only if x.seg_path_id not in finished_ops]
        self.remove_finished_segment(finished_op_loops | finished_ops)

    def remove_finished_segment(self, removed_segments):
        # self.log.debug("segments to be done: %s" % self.segments_to_be_done)
        # self.log.debug("removed segments: %s" % removed_segments)
        for x in removed_segments:
            self.segments_to_be_done.remove(x)
            if x in self.segments_resolving_deadlock:
                self.segments_resolving_deadlock.remove(x)
        # self.log.info("segments to be done: %s" % self.segments_to_be_done)

    def process_egress_link_segment(self, segments_by_seg_path_id, local_seg_path_ids,
                                    type_segments, local_link, src, dst):
        for u_op in local_link.to_adds + local_link.to_adds_only:
            assert isinstance(u_op, UpdateOperation)
            if u_op.seg_path_id not in local_seg_path_ids:
                # origin_op = segments_by_seg_path_id[u_op.seg_path_id]
                local_seg_path_ids[u_op.seg_path_id] = [(src, dst)]
            else:
                local_seg_path_ids[u_op.seg_path_id].append((src, dst))
            if type_segments.has_key(u_op.seg_path_id):
                type_segments[u_op.seg_path_id] |= 2
            else:
                type_segments[u_op.seg_path_id] = 2
            self.new_succ_by_seg_path_id[u_op.seg_path_id] = dst
            if dst == segments_by_seg_path_id[u_op.seg_path_id].end_sw_new:
                self.remaining_good_vol_to_move[u_op.seg_path_id] = \
                    segments_by_seg_path_id[u_op.seg_path_id].vol

        for u_op in local_link.to_removes:
            self.old_succ_by_seg_path_id[u_op.seg_path_id] = dst
            if u_op.seg_path_id not in local_seg_path_ids:
                # origin_op = segments_by_seg_path_id[u_op.seg_path_id]
                local_seg_path_ids[u_op.seg_path_id] = []
            if type_segments.has_key(u_op.seg_path_id):
                type_segments[u_op.seg_path_id] |= 1
            else:
                type_segments[u_op.seg_path_id] = 1

    def process_ingress_link_segment(self, local_link, src):
        for u_op in local_link.to_adds + local_link.to_adds_only:
            assert isinstance(u_op, UpdateOperation)
            self.new_pred_by_seg_path_id[u_op.seg_path_id] = src

        for u_op in local_link.to_removes:
            self.old_pred_by_seg_path_id[u_op.seg_path_id] = src

    def count_of_l_segment(self, local_segments):
        no_of_existing_flows = 0
        no_of_new_flows = 0
        for l_segment in local_segments.values():
            if l_segment.type_of_update == 1:
                no_of_existing_flows += 1
            elif l_segment.type_of_update == 2:
                no_of_new_flows += 1
            else:
                no_of_existing_flows += 1
                no_of_new_flows += 1
        self.trace.no_of_working_rules = no_of_existing_flows
        self.trace.max_no_of_working_rules = self.trace.no_of_working_rules

    def extract_local_segment(self, segments_by_seg_path_id, local_seg_path_ids, type_segments):
        segments = defaultdict()
        for seg_path_id in local_seg_path_ids.keys():
            segments_by_seg_path_id[seg_path_id].new_link_seg = local_seg_path_ids[seg_path_id]
            segments_by_seg_path_id[seg_path_id].type_of_update = type_segments[seg_path_id]
            segments[seg_path_id] = segments_by_seg_path_id[seg_path_id]
        return segments

    def create_local_dependency_graph(self, links_by_endpoint, segments_by_seg_path_id):
        debug = self.log.debug
        # --> every link already has the list of update_operations from path_to_ops_by_link function
        local_links = defaultdict(Link)
        # --> every update operation has:
        #   endpoints that helps to link to the links
        #   the list of link from switch that relevant to the update
        #   in the old segment (old_link_seg) and the new segment (new_link_seg) getting from segs
        local_seg_path_ids = {}#defaultdict(LinkSegment)
        type_segments = {}
        for nb in self.neighbor_ids:
            # now links_by_endpoint only includes link that are changed during the update
            # if (self.id, nb) not in links_by_endpoint:
            #     continue
            link = None
            if links_by_endpoint.has_key((self.id, nb)):
                link = links_by_endpoint[(self.id, nb)]
            reversed_link = None
            if links_by_endpoint.has_key((nb, self.id)):
                reversed_link = links_by_endpoint[(nb, self.id)]
            if link is not None:
                if len(link.to_adds) > 0 or len(link.to_adds_only) > 0 or len(link.to_removes) > 0:
                    local_links[(self.id, nb)] = link
                    self.process_egress_link_segment(segments_by_seg_path_id, local_seg_path_ids,
                                                 type_segments, link, self.id, nb)
                if reversed_link is not None:
                    self.process_ingress_link_segment(reversed_link, nb)

        if self.compute_critical_cycle:
            ez_flow_tool.prioritizing_update_segments(local_links, links_by_endpoint, segments_by_seg_path_id)

        self.links_by_endpoints = local_links
        self.segments_by_seg_path_id = \
            self.extract_local_segment(segments_by_seg_path_id, local_seg_path_ids, type_segments)
        self.segments_to_be_done = set([x for x in local_seg_path_ids.keys()])
        self.count_of_l_segment(self.segments_by_seg_path_id)
        # self.log.info("install update debug local_links %s" % self.links_by_endpoints)
        # self.log.info("install update debug local_segments %s" % self.segments_by_seg_path_id)
        # self.log.info("old_succ_by_seg_path: %s" % str(self.old_succ_by_seg_path_id))
        # self.log.info("old_pred_by_seg_path: %s" % str(self.old_pred_by_seg_path_id))
        # self.log.info("new_succ_by_seg_path: %s" % str(self.new_succ_by_seg_path_id))
        # self.log.info("new_pred_by_seg_path: %s" % str(self.new_pred_by_seg_path_id))
        # self.log.debug("install update debug segments to be done %s" % self.segments_to_be_done)

    def suspect_deadlock_on_link(self, link):
        if len(link.to_adds) + len(link.to_adds_loop) == 0:
            return
        suspecting_start = time()
        self.suspecting_deadlocks[(link.src, link.dst)] = suspecting_start
        link.scheduling_mode = constants.SUSPECTING_LINK
        # self.log.info("can_violate_congestion: %s" % self.can_violate_congestion)
        # self.log.info("segments to be done %s" % self.segments_to_be_done)
        if not self.can_violate_congestion:
            eventlet.spawn_after(constants.SUSPECTING_TIME, self.suspecting_time_expire, (link.src, link.dst))
        else:
            self.suspecting_deadlock_for_this_test = True
            eventlet.spawn_after(constants.SUSPECTING_TIME_SKIP_DEADLOCK, self.skipping_deadlock)

    def __do_update_on_removing_link__(self, update_infos, link, affected_links):
        changed_result = self.check_and_execute_all_updates_on_link(link, update_infos,
                                                                    affected_links)
        if changed_result == 0:
            self.suspect_deadlock_on_link(link)
        while len(affected_links) > 0:
            endpoints = affected_links.popleft()
            a_link = self.links_by_endpoints[endpoints]
            changed_result = self.check_and_execute_all_updates_on_link(a_link, update_infos,
                                                                        affected_links)
            if changed_result == 0:
                self.suspect_deadlock_on_link(a_link)


    def work_with_deadlock_link(self, link):
        update_infos = {}
        affected_links = deque([])
        if self.check_and_execute_all_updates_on_link(link, update_infos, affected_links) <= 0:
            eventlet.spawn_after(constants.SUSPECTING_TIME_SKIP_DEADLOCK, self.skipping_deadlock)
            # self.log.info("wait to violate congestion on link %d->%d: current_avail %f"
            #               % (link.src, link.dst, link.avail_cap))
            # self.log.info("segments to be done: %s" % self.segments_to_be_done)
            self.suspecting_deadlock_for_this_test = True
            return update_infos, affected_links
        elif link.scheduling_mode == constants.SPLITTING_LINK:
            eventlet.spawn_after(constants.SUSPECTING_TIME, self.work_with_deadlock_link, link)
        else:
            self.unsuspecting_link(link)
        return update_infos, affected_links

    def suspecting_time_expire(self, key):
        update_infos = {}
        affected_links = deque([])
        if self.suspecting_deadlocks.has_key(key):
            # self.suspecting_deadlocks.pop(key)
            link = self.links_by_endpoints[key]
            if len(link.to_adds) + len(link.to_adds_loop) == 0:
                return
            #self.log.info("start splitting volume on link %d->%d: current_avail %f"
            #              % (link.src, link.dst, link.avail_cap))
            #self.log.info("segments to be done: %s" % self.segments_to_be_done)
            # self.in_deadlocks.add(key)
            self.add_deadlock_link(key)
            update_infos, affected_links = self.work_with_deadlock_link(link)
        while len(affected_links) > 0:
            endpoints = affected_links.popleft()
            a_link = self.links_by_endpoints[endpoints]
            self.__do_update_on_removing_link__(update_infos, a_link, affected_links)
        if len(update_infos) > 0:
            self.callback_for_new_update(update_infos)

    def skipping_deadlock(self):
        if not self.suspecting_deadlock_for_this_test:
            return
        update_infos = {}
        affected_links = deque([])
        while len(self.suspecting_deadlocks) > 0:
            self.scheduling_mode |= constants.CONGESTION_MODE
            key = self.suspecting_deadlocks.keys()[0]
            link = self.links_by_endpoints[key]
            # self.log.info("start violating congestion on link %d->%d" % (link.src, link.dst))
            self.check_and_execute_all_updates_on_link(link, update_infos, affected_links)
            self.unsuspecting_link(link)
        while len(affected_links) > 0:
            endpoints = affected_links.popleft()
            a_link = self.links_by_endpoints[endpoints]
            self.__do_update_on_removing_link__(update_infos, a_link, affected_links)
        if len(update_infos) > 0:
            self.callback_for_new_update(update_infos)
