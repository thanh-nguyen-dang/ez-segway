from collections import deque, defaultdict
import itertools

from domain.message import *
from misc import logger, global_vars
# from misc.utils import SortedPair
from ez_lib.p2p_scheduler import P2PScheduler
from domain.network_premitives import LinkSegment, Link
from time import time
from collections import deque


class EzSwitchHandler(object):
    def __init__(self, id_, ctrl, neighbor_ids, callback_for_new_update):
        self.ctrl = ctrl
        self.id = id_
        self.received_install_msg = False
        self.new_pred_by_path_id = {}
        self.new_succ_by_path_id = {}
        self.old_pred_by_path_id = {}
        self.old_succ_by_path_id = {}
        self.received_allocated_msgs = deque([])
        self.received_good_to_move_msgs = deque([])
        self.received_removing_msgs = deque([])
        self.received_coherent_msgs = deque([])
        self.received_free_vol = {}
        self.log = self.init_logger(id_)
        self.scheduler = P2PScheduler(self.id, neighbor_ids, self.log, self.callback_from_scheduler_for_new_update)
        self.callback_for_new_update = callback_for_new_update

    @staticmethod
    def init_logger(id_):
        return logger.getLogger("SwitchHandler%d" % id_, constants.LOG_LEVEL)

    def do_install_update(self, msg):
        assert isinstance(msg, InstallUpdateMessage)
        self.scheduler.current_update = msg.update_id
        self.scheduler.reset()
        self.scheduler.can_violate_congestion = msg.skip_deadlock
        self.scheduler.create_local_dependency_graph(msg.link_by_endpoints,
                                                     msg.segments_by_seg_path_id)
        self.__store_pred_succ_by_flow__(msg)
        self.received_install_msg = True
        self.scheduler.compute_required_vol_for_dependency_loop()
        time_finishing_dependency_graph = time() * 1000

        update_infos = {}

        removed_segments = []
        affected_links = deque([])
        removed_segments.extend(self.scheduler.execute_all_remove_only_updates(update_infos, affected_links))
        while len(affected_links) > 0:
            endpoints = affected_links.popleft()
            a_link = self.scheduler.links_by_endpoints[endpoints]
            self.__do_update_on_removing_link__(update_infos, a_link, affected_links)

        while len(self.received_removing_msgs) > 0:
            msg = self.received_removing_msgs.popleft()
            self.__do_handle_removing_msg__(msg, update_infos, removed_segments, affected_links)

        self.scheduler.check_and_execute_all_updates(update_infos, affected_links)
        self.log.debug(update_infos)

        # while len(self.received_allocated_msgs) > 0:
        #     msg = self.received_allocated_msgs.popleft()
        #     self.__do_handler_allocated_msg__(msg, removed_segments)

        while len(self.received_good_to_move_msgs) > 0:
            msg = self.received_good_to_move_msgs.popleft()
            self.__do_handler_good_to_move_msg__(msg, update_infos, removed_segments)

        self.scheduler.remove_finished_segment(removed_segments)

        self.scheduler.extend_update_infos(update_infos,
                                           self.__send_coherent_msg_from_end_sw_path__()
                                           )
        self.log.debug(update_infos.values())
        return update_infos.values(), self.__is_finished(), time_finishing_dependency_graph

    def __is_finished(self):
        # self.log.debug("removing msgs: %s" % self.received_removing_msgs)
        # self.log.debug("good_to_move msgs: %s" % self.received_good_to_move_msgs)
        # self.log.debug("coherent msgs: %s" % self.received_coherent_msgs)
        # self.log.info("no of segments left: %d" % len(self.scheduler.segments_to_be_done))
        # lst_seg_path_ids = ""
        # for seg_path_id in self.scheduler.segments_to_be_done:
        #     lst_seg_path_ids += str(seg_path_id) + ", "
        # self.log.info("segment is not executed: %s" % lst_seg_path_ids)
        if len(self.received_removing_msgs) == 0 \
                and len(self.received_good_to_move_msgs) == 0 \
                and len(self.received_allocated_msgs) == 0 \
                and len(self.received_coherent_msgs) == 0 \
                and len(self.scheduler.segments_to_be_done) == 0:
            return 1
        return 0

    # Handling all notification messages
    def do_handle_notification(self, msg):
        self.scheduler.trace.no_of_received_messages += 1
        if self.scheduler.current_update == msg.update_id and \
                msg.seg_path_id not in self.scheduler.segments_to_be_done:
            return [], -1
        if msg.msg_type == constants.GOOD_TO_MOVE_MSG:
            return self.handle_good_to_move_msg(msg), self.__is_finished()
        elif msg.msg_type == constants.COHERENT_MSG:
            self.scheduler.coherent_succs.add(msg.seg_path_id)
            return self.handle_coherent_msg(msg), self.__is_finished()
        elif msg.msg_type == constants.REMOVING_MSG:
            self.scheduler.removing_preds.add(msg.seg_path_id)
            return self.handle_removing_msg(msg), self.__is_finished()
        return [], self.__is_finished()

    # only intermediate switch of the old path and ending switch of a segment receive removing message
    def handle_removing_msg(self, msg):
        update_infos = {}
        removed_segments = []
        affected_links = deque([])
        self.__do_handle_removing_msg__(msg, update_infos, removed_segments, affected_links)
        self.scheduler.remove_finished_segment(removed_segments)
        return update_infos.values()

    def handle_good_to_move_msg(self, msg):
        update_infos = {}
        removed_segments = []
        self.__do_handler_good_to_move_msg__(msg, update_infos, removed_segments)
        self.scheduler.remove_finished_segment(removed_segments)
        return update_infos.values()

    def handle_coherent_msg(self, msg):
        update_infos = {}
        self.__do_handle_coherent_msg__(msg, update_infos)
        return update_infos.values()

    def __do_update_on_removing_link__(self, update_infos, link, affected_links):
        changed_result = self.scheduler.check_and_execute_all_updates_on_link(link, update_infos,
                                                                              affected_links)
        if changed_result == 0:
            self.scheduler.suspect_deadlock_on_link(link)
        while len(affected_links) > 0:
            endpoints = affected_links.popleft()
            a_link = self.scheduler.links_by_endpoints[endpoints]
            changed_result = self.scheduler.check_and_execute_all_updates_on_link(a_link, update_infos,
                                                                                  affected_links)
            if changed_result == 0:
                self.scheduler.suspect_deadlock_on_link(a_link)
                # self.__do_update_on_removing_link__(update_infos, a_link, affected_links)


    def __do_handle_removing_msg__(self, msg, update_infos, removed_segments, affected_links):
        if self.scheduler.current_update != msg.update_id:
            self.received_removing_msgs.append(msg)
            return
        succ_id, remaining_vol = self.scheduler.release_capacity_by_removing_msg(msg)
        # self.scheduler.check_and_execute_all_updates(update_infos)
        if succ_id is not None and succ_id != constants.ID_NULL:
            end_points = (self.id, succ_id)
            link = self.scheduler.links_by_endpoints[end_points]
            update_next = self.scheduler.create_update_next(constants.REMOVE_NEXT,
                                                            msg.seg_path_id)
            l_segment = self.scheduler.segments_by_seg_path_id[msg.seg_path_id]
            if succ_id != l_segment.end_sw_old:
                new_msg = self.scheduler.create_new_msg_to_sw_with_id(succ_id,
                                                                  constants.REMOVING_MSG,
                                                                  msg.seg_path_id, 0)
                self.scheduler.append_update_infos(update_infos, ([new_msg], update_next))
            else:
                self.scheduler.append_update_infos(update_infos, ([], update_next))
            # affected_links = deque([])
            self.__do_update_on_removing_link__(update_infos, link, affected_links)
        if remaining_vol == 0 or remaining_vol == None:
            removed_segments.append(msg.seg_path_id)

        # self.log.debug("Update infos: %s" % update_infos.values())
        return

    def __do_handler_good_to_move_msg__(self, msg, update_infos, removed_segments):
        if self.scheduler.current_update != msg.update_id:
            self.received_good_to_move_msgs.append(msg)
            return
        end_points = (self.id, msg.src_id)
        self.scheduler.good_to_move_succs.add(msg.seg_path_id)
        finished_list = set([])
        affected_links = deque([])
        self.log.debug(finished_list)
        self.log.debug(update_infos)
        link = self.scheduler.links_by_endpoints[end_points]
        l_segment = self.scheduler.segments_by_seg_path_id[msg.seg_path_id]
        if msg.split_vol < 0:
            self.scheduler.remaining_good_vol_to_move[msg.seg_path_id] = l_segment.vol
            (new_msgs, update_next) = self.scheduler.check_and_execute_update_on_link(
                    link, msg.seg_path_id, finished_list, affected_links)
            self.scheduler.append_update_infos(update_infos, (new_msgs, update_next))
        else:
            self.scheduler.remaining_good_vol_to_move[msg.seg_path_id] = msg.split_vol
            (new_msgs, update_next) = self.scheduler.check_and_execute_update_on_link(
                    link, msg.seg_path_id, finished_list, affected_links, msg.split_vol)
            self.scheduler.append_update_infos(update_infos, (new_msgs, update_next))
        # self.log.info("update_next: %s" % update_next)
        if update_next != None:
            self.scheduler.remove_finished_ops(link, finished_list, finished_list)
        else:
            self.scheduler.suspect_deadlock_on_link(link)
        if len(affected_links) > 0:
            endpoints = affected_links.popleft()
            a_link = self.scheduler.links_by_endpoints[endpoints]
            self.__do_update_on_removing_link__(update_infos, a_link, affected_links)
        return

    # def __do_handler_allocated_msg__(self, msg, removed_segments):
    #     if self.scheduler.current_update != msg.update_id:
    #         self.received_allocated_msgs.append(msg)
    #         return
    #     self.scheduler.execute_by_seg_path_id(msg.seg_path_id)
    #     removed_segments.append(msg.seg_path_id)

    def __do_handle_coherent_msg__(self, msg, update_infos):
        (new_succ, new_path_coherent) = (None, None)
        if self.new_succ_by_path_id.has_key(msg.seg_path_id):
            (new_succ, new_path_coherent) = self.new_succ_by_path_id[msg.seg_path_id]
        (old_succ, old_path_coherent) = (None, None)
        if self.old_succ_by_path_id.has_key(msg.seg_path_id):
            (old_succ, old_path_coherent) = self.old_succ_by_path_id[msg.seg_path_id]

        if (msg.src_id == new_succ and old_path_coherent) \
                or (msg.src_id == old_succ and new_path_coherent) \
                or (new_succ == old_succ):
            new_pred, new_res = self.new_pred_by_path_id[msg.seg_path_id]
            old_pred, old_res = self.old_pred_by_path_id[msg.seg_path_id]
            if new_pred is not None:
                new_msg = self.scheduler.create_new_msg_to_sw_with_id(new_pred, constants.COHERENT_MSG,
                                                                      msg.seg_path_id, 0)
                self.scheduler.append_update_infos(update_infos, ([new_msg], None))
            if old_pred is not None and old_pred != new_pred:
                new_msg = self.scheduler.create_new_msg_to_sw_with_id(old_pred, constants.COHERENT_MSG,
                                                                      msg.seg_path_id, 0)
                self.scheduler.append_update_infos(update_infos, ([new_msg], None))
        elif msg.src_id == new_succ:
            self.new_succ_by_path_id[msg.seg_path_id] = (new_succ, True)
        elif msg.src_id == old_succ:
            self.old_succ_by_path_id[msg.seg_path_id] = (old_succ, True)

    def __send_coherent_msg_from_end_sw_path__(self):
        msgs = []
        for key in self.new_succ_by_path_id.keys():
            if self.new_pred_by_path_id.has_key(key) and self.new_succ_by_path_id[key] == (None, None) \
                    and not (self.new_pred_by_path_id[key] == (None, None)):
                new_pred, res = self.new_pred_by_path_id[key]
                msgs.append(self.scheduler.create_new_msg_to_sw_with_id(new_pred, constants.COHERENT_MSG, key, 0))
        return msgs, []

    def __store_pred_succ_by_flow__(self, msg):
        self.new_pred_by_path_id = msg.new_pred_by_flow
        self.new_succ_by_path_id = msg.new_succ_by_flow
        self.old_pred_by_path_id = msg.old_pred_by_flow
        self.old_succ_by_path_id = msg.old_succ_by_flow

    def callback_from_scheduler_for_new_update(self, update_infos):
        self.callback_for_new_update(update_infos.values(), self.__is_finished())
