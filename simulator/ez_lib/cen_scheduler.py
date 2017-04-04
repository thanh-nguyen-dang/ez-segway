import itertools

from ez_lib import ez_flow_tool
from collections import defaultdict
from ez_scheduler import EzScheduler
from ez_lib.ez_ob import CenUpdateInfo, UpdateNext
from misc import constants, logger
from domain.message import *
from collections import deque
from misc import global_vars
import time
import eventlet


class CenCtrlScheduler(EzScheduler):
    def __init__(self, switches_, log_):
        self.switches = switches_
        super(CenCtrlScheduler, self).__init__(0, log_)
        self.remaining_vol_of_dependency_loop_on_link = {}
        self.received_updated_msg = defaultdict()
        self.received_removed_msg = defaultdict()
        ########## Begin three properties are used for parallel processes ##########
        self.no_of_pending_msgs = {}
        self.notification_queues = {x: deque([]) for x in self.switches}
        self.current_notification_time = {x: -1 for x in self.switches}
        self.current_processing_time = {x: -1 for x in self.switches}
        ########### End three properties are used for parallel processes ###########
        self.to_sames = defaultdict(list)
        self.encounter_deadlock = False
        self.do_segmentation = True

    def reset(self):
        super(CenCtrlScheduler, self).reset()
        self.remaining_vol_of_dependency_loop_on_link = {}
        self.received_updated_msg = defaultdict()
        self.received_removed_msg = defaultdict()
        ########## Begin three properties are used for parallel processes ##########
        self.no_of_pending_msgs = {}
        self.notification_queues = {x: deque([]) for x in self.switches}
        self.current_notification_time = {x: -1 for x in self.switches}
        self.current_processing_time = {x: -1 for x in self.switches}
        ########### End three properties are used for parallel processes ###########
        self.to_sames = defaultdict(list)
        self.encounter_deadlock = False
        self.do_segmentation = True

    def __str__(self):
        return "Centralized Controller"

    @staticmethod
    def init_logger():
        return logger.getLogger("Centralized Controller", constants.LOG_LEVEL)

    def create_dependency_graph(self, old_flows, new_flows):
        time_start_computing = time.time() * 1000
        ez_flow_tool.create_dependency_graph(old_flows, new_flows,
                                             self.links_by_endpoints, self.segments_by_seg_path_id,
                                             self.to_sames, do_segmentation=self.do_segmentation)
        self.find_dependency_loop_and_sort_updates(self.links_by_endpoints, self.segments_by_seg_path_id)
        self.log.debug(self.links_by_endpoints)
        self.log.debug(self.segments_by_seg_path_id)
        # self.log.info("time to compute dependency graph: %s" % str(time() * 1000 - time_start_computing))

    def process_coherent(self):
        send_to_sames = set()
        for key in self.to_sames.keys():
            to_same = self.to_sames[key]
            for sw in to_same:
                send_to_sames.add(sw)
        # for sw in send_to_sames:
        #     msg = NotificationMessage(0, sw, constants.COHERENT_MSG, 0)
        #     self.send_to_switch(msg, sw)

    def compute_required_vol_for_dependency_loop(self, link):
        self.remaining_vol_of_dependency_loop_on_link[(link.src, link.dst)] = 0
        for add_op in link.to_adds_loop:
            self.remaining_vol_of_dependency_loop_on_link[(link.src, link.dst)] \
                += self.segments_by_seg_path_id[add_op.seg_path_id].vol

    def find_dependency_loop_and_sort_updates(self, links_by_endpoints, segments_by_seg_path_id):
        # pool = eventlet.GreenPool()
        for sw in self.switches:
            # pool.spawn_n(self.find_dependency_loop_and_sort_updates_by_sw, sw,
            #              links_by_endpoints, segments_by_seg_path_id)
            self.find_dependency_loop_and_sort_updates_by_sw(sw, links_by_endpoints, segments_by_seg_path_id)
        # pool.waitall()

        # for link in links_by_endpoints.values():
        #     ez_flow_tool.compute_scheduling_info_for_a_link(link, links_by_endpoints, segments_by_seg_path_id)
        # global_vars.finish_prioritizing_time = time.clock()

    def find_dependency_loop_and_sort_updates_by_sw(self, sw, links_by_endpoints, segments_by_seg_path_id):
        for link in links_by_endpoints.values():
            if link.src == sw:
                ez_flow_tool.find_dependency_loop_for_link(link, links_by_endpoints, segments_by_seg_path_id)
        for link in links_by_endpoints.values():
            if link.src == sw:
                self.compute_required_vol_for_dependency_loop(link)
        current_time = time.clock()
        if global_vars.finish_computation_time < current_time:
            global_vars.finish_computation_time = time.clock()

    def execute_all_remove_only_updates(self, update_infos):
        for l_segment in self.segments_by_seg_path_id.values():
            old_sws = set(l_segment.old_seg)
            old_sws.add(l_segment.init_sw)

            seg_path_id = l_segment.seg_path_id
            self.received_removed_msg[seg_path_id] = set()
            if l_segment.remove_only:
                if not update_infos.has_key(seg_path_id):
                    update_infos[seg_path_id] = CenUpdateInfo(seg_path_id,
                                                              l_segment.flow_src,
                                                              l_segment.flow_dst)
                for sw in old_sws:
                    update_infos[seg_path_id].update_nexts[sw] = UpdateNext(l_segment.seg_path_id,
                                                                            sw, constants.REMOVE_NEXT)
                l_segment.update_status = constants.SENT_REMOVING

    def update_message_queues(self, update_infos, process_update_info_func):
        increased = set()
        related_sws = set([])
        for key in update_infos.keys():
            update_info = update_infos[key]
            # self.logger.info("Process update info %s at %d ms from starting" % (update_info, (time() - self.current_start_time)*1000))
            assert update_info, CenUpdateInfo
            for sw in update_infos[key].update_nexts.keys():
                if sw not in increased:
                    self.current_notification_time[sw] += 1
                    increased.add(sw)
                    self.no_of_pending_msgs[(sw, self.current_notification_time[sw])] = 0
                #update_next = update_info.update_nexts[sw]
                process_update_info_func(sw, update_info)
                self.log.debug("add message in processing update_info: %s" % update_info)
                self.log.debug("pending messages: %s" % str(self.no_of_pending_msgs))
                related_sws.add(sw) #self.datapaths[sw + 1])
        return related_sws

    def increase_processing_time(self, sw):
        self.current_processing_time[sw] += 1

    def enque_msg_to_notification_queue(self, sw, msg):
        self.notification_queues[sw].append(msg)
        self.no_of_pending_msgs[(sw, self.current_notification_time[sw])] += 1

    def deque_msg_from_notification_queue(self, sw):
        msg = self.notification_queues[sw].popleft()
        self.no_of_pending_msgs[(sw, self.current_processing_time[sw])] -= 1
        return msg

    def has_pending_msg_of_sw(self, sw):
        return self.no_of_pending_msgs[(sw, self.current_processing_time[sw])] > 0

    # def check_all_capable_for_link(self, link, executable_segments_by_link):
    #     capable_segments = []
    #     done_loop = True
    #     endpoints = (link.src, link.dst)
    #     total_vol = 0
    #     for op in link.to_adds_loop:
    #         l_segment = self.segments_by_seg_path_id[op.seg_path_id]
    #         if l_segment.update_status == constants.NOTHING:
    #             done_loop = False
    #             total_vol += l_segment.vol
    #
    # def check_and_send_possible_update_by_link(self, update_infos):
    #     executable_segments_by_link = {}
    #     executable_link_by_segments = {}
    #     for link in self.links_by_endpoints.values():
    #         self.check_all_capable_for_link(link, executable_segments_by_link)

    def total_pending_cycle_vol(self, link):
        total_vol = 0
        for add_op in link.to_adds + link.to_adds_loop + link.to_adds_only:
            total_vol += self.segments_by_seg_path_id[add_op.seg_path_id].vol
        return total_vol

    def check_to_split(self, link, l_segment):
        pass

    def splittable_vol(self, seg_path_id):
        # TODO: Update remaining_vol_of_loop when adding or removing segment
        final_split_vol = 0
        l_segment = self.segments_by_seg_path_id[seg_path_id]
        for endpoints in l_segment.new_link_seg:
            link = self.links_by_endpoints[endpoints]
            is_add_only = False
            for op in link.to_adds_only:
                if op.seg_path_id == seg_path_id:
                    return 0
            splittable, split_vol = self.check_to_split(link, l_segment)
            if splittable and final_split_vol > split_vol > 0:
                final_split_vol = split_vol

        self.log.debug("capable %s" % l_segment)
        return final_split_vol

    def check_and_send_possible_split_updates(self, update_infos):
        has_execution = True
        while has_execution:
            has_execution = False
            for l_segment in self.segments_by_seg_path_id.values():
                if l_segment.update_status != constants.NOTHING:
                    continue
                seg_path_id = l_segment.seg_path_id
                self.log.debug(l_segment)
                split_vol = self.splittable_vol(l_segment.seg_path_id)
                if split_vol > 0:
                    if not update_infos.has_key(seg_path_id):
                        update_infos[seg_path_id] = CenUpdateInfo(seg_path_id,
                                                                  l_segment.flow_src,
                                                                  l_segment.flow_dst)
                    update_info = update_infos[seg_path_id]
                    update_info.update_nexts[l_segment.init_sw] = UpdateNext(seg_path_id,
                                                                             l_segment.new_seg[0],
                                                                             constants.UPDATE_NEXT)
                    for i in range(len(l_segment.new_seg) - 1):
                        # self.log.debug("send to sw%s" % str(l_segment.new_seg[i]))
                        next_sw = l_segment.new_seg[i + 1]
                        update_info.update_nexts[l_segment.new_seg[i]] = UpdateNext(seg_path_id,
                                                                                    next_sw,
                                                                                    constants.ADD_NEXT)
                    self.received_updated_msg[l_segment.seg_path_id] = set()
                    l_segment.update_status = constants.SENT_ADDING
                    l_segment.is_splitting = True

                    for pair in l_segment.new_link_seg:
                        self.log.info("avail_cap of link %s: %f, "
                                      "give %f to segment %s" % (str(pair),
                                                                 self.links_by_endpoints[pair].avail_cap,
                                                                 l_segment.vol,
                                                                 str(l_segment.seg_path_id)))
                        self.links_by_endpoints[pair].avail_cap -= split_vol
                        for u_op in self.links_by_endpoints[pair].to_adds_loop:
                            if u_op.seg_path_id == l_segment.seg_path_id:
                                self.remaining_vol_of_dependency_loop_on_link[pair] -= split_vol
            count = 0
            for l_segment in self.segments_by_seg_path_id.values():
                if l_segment.update_status == constants.NOTHING:
                    count += 1
            self.log.debug("number of flows that is not done anything %d" % count)

    def check_possible_update_by_links(self, update_infos):
        has_execution = True
        while has_execution:
            has_execution = False
            for l_segment in self.segments_by_seg_path_id.values():
                if l_segment.update_status != constants.NOTHING:
                    continue
                seg_path_id = l_segment.seg_path_id
                self.log.debug(l_segment)
                if self.is_capable(l_segment.seg_path_id) or self.encounter_deadlock:
                    if not update_infos.has_key(seg_path_id):
                        update_infos[seg_path_id] = CenUpdateInfo(seg_path_id,
                                                                  l_segment.flow_src,
                                                                  l_segment.flow_dst)
                    update_info = update_infos[seg_path_id]
                    update_info.update_nexts[l_segment.init_sw] = UpdateNext(seg_path_id,
                                                                             l_segment.new_seg[0],
                                                                             constants.UPDATE_NEXT)
                    for i in range(len(l_segment.new_seg) - 1):
                        next_sw = l_segment.new_seg[i + 1]
                        update_info.update_nexts[l_segment.new_seg[i]] = UpdateNext(seg_path_id,
                                                                                    next_sw,
                                                                                    constants.ADD_NEXT)
                    self.received_updated_msg[l_segment.seg_path_id] = set()
                    l_segment.update_status = constants.SENT_ADDING
                    for pair in l_segment.new_link_seg:
                        self.links_by_endpoints[pair].avail_cap -= l_segment.vol
                        for u_op in self.links_by_endpoints[pair].to_adds_loop:
                            if u_op.seg_path_id == l_segment.seg_path_id:
                                self.remaining_vol_of_dependency_loop_on_link[pair] -= l_segment.vol
            count = 0
            for l_segment in self.segments_by_seg_path_id.values():
                if l_segment.update_status == constants.NOTHING:
                    count += 1
            self.log.debug("number of flows that is not done anything %d" % count)

    def check_and_send_possible_updates(self, update_infos):
        has_execution = True
        while has_execution:
            has_execution = False
            for l_segment in self.segments_by_seg_path_id.values():
                if l_segment.update_status != constants.NOTHING:
                    continue
                seg_path_id = l_segment.seg_path_id
                self.log.debug(l_segment)
                if self.is_capable(l_segment.seg_path_id) or self.encounter_deadlock:
                    if not update_infos.has_key(seg_path_id):
                        update_infos[seg_path_id] = CenUpdateInfo(seg_path_id,
                                                                  l_segment.flow_src,
                                                                  l_segment.flow_dst)
                    update_info = update_infos[seg_path_id]
                    update_info.update_nexts[l_segment.init_sw] = UpdateNext(seg_path_id,
                                                                             l_segment.new_seg[0],
                                                                             constants.UPDATE_NEXT)
                    for i in range(len(l_segment.new_seg) - 1):
                        next_sw = l_segment.new_seg[i + 1]
                        update_info.update_nexts[l_segment.new_seg[i]] = UpdateNext(seg_path_id,
                                                                                    next_sw,
                                                                                    constants.ADD_NEXT)
                    self.received_updated_msg[l_segment.seg_path_id] = set()
                    l_segment.update_status = constants.SENT_ADDING
                    for pair in l_segment.new_link_seg:
                        self.links_by_endpoints[pair].avail_cap -= l_segment.vol
                        for u_op in self.links_by_endpoints[pair].to_adds_loop:
                            if u_op.seg_path_id == l_segment.seg_path_id:
                                self.remaining_vol_of_dependency_loop_on_link[pair] -= l_segment.vol
            count = 0
            for l_segment in self.segments_by_seg_path_id.values():
                if l_segment.update_status == constants.NOTHING:
                    count += 1
            self.log.debug("number of flows that is not done anything %d" % count)

    def check_and_do_next_update(self, msg):
        update_infos = defaultdict(CenUpdateInfo)
        if not self.received_updated_msg.has_key(msg.seg_path_id):
            self.received_updated_msg[msg.seg_path_id] = set()
        self.received_updated_msg[msg.seg_path_id].add(msg.src_id)
        self.log.debug("handle updated msg %s" % msg)

        assert self.segments_by_seg_path_id.has_key(msg.seg_path_id), True
        link_segment = self.segments_by_seg_path_id[msg.seg_path_id]
        # self.log.info("receive updated msgs for segment %s, new_seg_length = %d"
        #               % (str(link_segment.seg_path_id), len(link_segment.new_seg)))
        if link_segment.update_status == constants.SENT_ADDING \
                and len(self.received_updated_msg[msg.seg_path_id]) == \
                                len(link_segment.new_seg):
            self.finish_adding_new_path(link_segment, update_infos)
        return update_infos

    def finish_adding_new_path(self, link_segment, update_infos):
        self.trace.time_using_new_path_by_seg_path_id[link_segment.seg_path_id] = time.time() * 1000
        if len(link_segment.old_seg) < 1:
            link_segment.update_status = constants.FINISH_ALL
        else:
            # self.log.info("receive enough updated msgs for segment %s" % str(link_segment.seg_path_id))
            link_segment.update_status = constants.FINISH_ADDING
            self.release_capacity_send_remove_msg_to_old_segment(update_infos, link_segment)

    def remove_segment_and_check_to_update(self, msg):
        assert isinstance(msg, NotificationMessage)
        update_infos = defaultdict(CenUpdateInfo)
        self.log.debug("handle removed msg %s" % msg)
        self.received_removed_msg[msg.seg_path_id].add(msg.src_id)

        link_segment = self.segments_by_seg_path_id[msg.seg_path_id]
        next_idx = 0
        if msg.src_id != link_segment.init_sw:
            next_idx = link_segment.old_seg.index(msg.src_id) + 1

        if next_idx < len(link_segment.old_seg):
            dst = link_segment.old_seg[next_idx]
            pair = (msg.src_id, dst)
            self.links_by_endpoints[pair].avail_cap += link_segment.vol
            # self.log.info("avail_cap of link %d->%d: %f, "
            #               "get from segment %s" % (msg.src_id, dst,
            #                                        self.links_by_endpoints[pair].avail_cap,
            #                                        str(link_segment.seg_path_id)))

        if len(self.received_removed_msg[msg.seg_path_id]) >= len(link_segment.old_seg) - 1:
            link_segment.update_status = constants.FINISH_ALL
            self.log.debug("finish %s" % str(link_segment.seg_path_id))
        self.check_and_send_possible_updates(update_infos)
        return update_infos

    def check_finish_update(self):
        count = 0
        finished = True

        for link_segment in self.segments_by_seg_path_id.values():
            if link_segment.update_status != constants.FINISH_ALL:
                update_status = ''
                if link_segment.update_status == constants.NOTHING:
                    count += 1
                    update_status = "NOTHING"
                if link_segment.update_status == constants.SENT_ADDING:
                    self.log.debug("must receive %d more UPDATED msgs" % (len(link_segment.new_seg)-1))
                    self.log.debug("received from: %s" % self.received_updated_msg[link_segment.seg_path_id])
                    update_status = "SENT_ADDING"
                elif link_segment.update_status == constants.SENT_REMOVING:
                    self.log.debug("must receive %d more REMOVED msgs" % (len(link_segment.old_seg)-1))
                    self.log.debug("received from: %s" % self.received_removed_msg[link_segment.seg_path_id])
                    update_status = "SENT REMOVING"
                elif link_segment.update_status == constants.FINISH_ADDING:
                    update_status = "FINISH_ADDING"
                elif link_segment.update_status == constants.FINISH_REMOVING:
                    update_status = "FINISH_REMOVING"
                self.log.debug("segment %s is not finished! update_status %s." % (str(link_segment.seg_path_id), update_status))
                # return False
                finished = False
                break
        has_no_pending_barrier = self.has_not_pending_msg()
        if not has_no_pending_barrier:
            return constants.ON_GOING
        elif not finished:
            self.log.debug("number of flows that is not done anything %d" % count)
            self.scheduling_mode = constants.CONGESTION_MODE
            return constants.ENCOUNTER_DEADLOCK
        else:
            current_mode = self.scheduling_mode
            self.scheduling_mode = constants.NORMAL_MODE
            if current_mode == constants.CONGESTION_MODE:
                return constants.FINISHED_WITH_DEADLOCK
            else:
                return constants.FINISHED_WITHOUT_DEADLOCK

    def has_not_pending_msg(self):
        self.log.debug("pending queue: %s" % str(self.no_of_pending_msgs))
        for queue_len in self.no_of_pending_msgs.values():
            if queue_len > 0:
                return False
        return True

    def release_capacity_send_remove_msg_to_old_segment(self, update_infos, l_segment):
        seg_path_id = l_segment.seg_path_id
        if not update_infos.has_key(seg_path_id):
            update_infos[seg_path_id] = CenUpdateInfo(seg_path_id, l_segment.flow_src,
                                                                l_segment.flow_dst)

        pair = (l_segment.init_sw, l_segment.old_seg[0])
        self.links_by_endpoints[pair].avail_cap += l_segment.vol
        # self.log.info("avail_cap of link %d->%d: %f, "
        #               "get from segment %s" % (l_segment.init_sw,
        #                                        l_segment.old_seg[0],
        #                                        self.links_by_endpoints[pair].avail_cap,
        #                                        str(l_segment.seg_path_id)))
        if len(l_segment.old_seg) > 1:
            for i in range(len(l_segment.old_seg) - 1):
                # self.log.debug("send to: %s" % l_segment.old_seg[i])
                next_sw = l_segment.old_seg[i + 1]
                update_infos[seg_path_id].update_nexts[l_segment.old_seg[i]] = UpdateNext(seg_path_id,
                                                                                          next_sw,
                                                                                          constants.REMOVE_NEXT)
            self.received_removed_msg[l_segment.seg_path_id] = set()
            l_segment.update_status = constants.SENT_REMOVING
        else:
            l_segment.update_status = constants.FINISH_ALL

    def are_all_moving_in_ops_finished(self, link):
        for u_op in link.to_adds + link.to_adds_loop:
            current_state = self.segments_by_seg_path_id[u_op.seg_path_id].update_status
            if current_state == constants.NOTHING \
                or current_state == constants.SENT_ADDING:
                return False
        return True

    def is_capable(self, seg_path_id):
        # TODO: Update remaining_vol_of_loop when adding or removing segment
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

            if (not is_dependency_loop_op and (link.avail_cap - l_segment.vol
                                                   < self.remaining_vol_of_dependency_loop_on_link[endpoints])) \
                    or (is_dependency_loop_op and link.avail_cap < l_segment.vol)\
                    or (is_add_only and (not self.are_all_moving_in_ops_finished(link)
                        or link.avail_cap < l_segment.vol)):
                return False
        self.log.debug("capable %s" % l_segment)
        return True
