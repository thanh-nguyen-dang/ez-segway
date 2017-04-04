from ez_lib.ez_ob import CenUpdateInfo
from domain.network_premitives import *
from cen_scheduler import CenCtrlScheduler
from time import time

class CenCtrlHandler(object):
    def __init__(self, switches_, log_):
        self.log = log_
        self.scheduler = CenCtrlScheduler(switches_, self.log)
        self.message_count = 0

    def __str__(self):
        return "Centralized Controller"

    # @staticmethod
    # def init_logger():
    #     return logger.getLogger("Centralized Controller", constants.LOG_LEVEL)

    def do_install_update(self, old_flows, new_flows):
        self.scheduler.reset()
        self.scheduler.create_dependency_graph(old_flows, new_flows)
        time_finishing_dependency_graph = time()
        update_infos = {}
        self.message_count = 0
        self.scheduler.execute_all_remove_only_updates(update_infos)
        result = self.scheduler.check_and_send_possible_updates(update_infos)
        # self.scheduler.process_coherent()
        return update_infos, time_finishing_dependency_graph

    def do_skip_deadlock(self):
        update_infos = {}
        self.scheduler.encounter_deadlock = True
        self.scheduler.check_and_send_possible_updates(update_infos)
        return update_infos

    def do_handle_feedback(self, msg):
        self.message_count += 1
        if msg.msg_type == constants.UPDATED_MSG:
            return self.__handle_updated_msg__(msg)
        elif msg.msg_type == constants.REMOVED_MSG:
            return self.__handle_removed_msg__(msg)
        elif msg.msg_type == constants.COHERENT_MSG:
            return self.__handle_coherent_msg__(msg)

    def do_handle_barrier_from_sw(self, sw, process_update_info_func, send_barrier_func, finish_update_func):
        self.scheduler.increase_processing_time(sw)

        # self.logger.debug("queue length: %d" % len(notification_queue))
        # self.logger.debug("count: %d" % self.no_of_pending_msgs[(dpid, self.current_processing_time[dpid])])
        all_update_infos = {}
        # receiving_time = time() * 1000
        while self.scheduler.has_pending_msg_of_sw(sw):
            # msg = notification_queue.popleft()
            # self.no_of_pending_msgs[(dpid, self.current_processing_time[dpid])] -= 1
            msg = self.scheduler.deque_msg_from_notification_queue(sw)
            # elapsed_time = receiving_time - msg.sending_time
            # self.logger.info("receive feedback message %s after %s ms transferring" % (msg, str(elapsed_time)))
            update_infos = self.do_handle_feedback(msg)
            for key in update_infos.keys():
                all_update_infos[key] = update_infos[key]
        self.handle_new_update_infos(all_update_infos, process_update_info_func, send_barrier_func)
        self.__check_to_stop_or_deadlock(process_update_info_func, send_barrier_func, finish_update_func)

    def handle_new_update_infos(self, update_infos, process_update_info_func, send_barrier_func):
        related_sws = self.scheduler.update_message_queues(update_infos,
                                                           process_update_info_func)
        send_barrier_func(related_sws)

    def __check_to_stop_or_deadlock(self, process_update_info_func, send_barrier_func, finish_update_func):
        finished_all_updates = self.scheduler.check_finish_update()
        # no_pending_msg = self.scheduler.has_not_pending_msg()
        # log.info("finished all updates: %s" % finished_all_updates)
        # log.info("no pending msg: %s" % no_pending_msg)
        if finished_all_updates == constants.FINISHED_WITH_DEADLOCK:
            finish_update_func(True)
        elif finished_all_updates == constants.FINISHED_WITHOUT_DEADLOCK:
            finish_update_func(False)
        elif finished_all_updates == constants.ENCOUNTER_DEADLOCK:
            self.log.info("deadlock")
            update_infos = self.do_skip_deadlock()
            self.handle_new_update_infos(update_infos, process_update_info_func, send_barrier_func)
            # self.__check_to_stop_or_deadlock(process_update_info_func, send_barrier_func, finish_update_func)
        # return self.scheduler.check_finish_update()

    def __handle_updated_msg__(self, msg):
        return self.scheduler.check_and_do_next_update(msg)

    def __handle_removed_msg__(self, msg):
        return self.scheduler.remove_segment_and_check_to_update(msg)

    def __handle_coherent_msg__(self, msg):
        self.log.debug("handle coherent msg %s" % msg)
        return defaultdict(CenUpdateInfo)
