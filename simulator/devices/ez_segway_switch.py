from ez_lib import ez_switch_handler
from ez_lib.ez_ob import P2PUpdateInfo
from devices.switch import *


class EzSegwaySwitch(Switch):
    def __init__(self, id_, ctrl, neighbor_ids):
        super(EzSegwaySwitch, self).__init__(id_, ctrl, neighbor_ids)
        self.handler = ez_switch_handler.EzSwitchHandler(id_, ctrl, neighbor_ids, self.callback_func)

    def install_update(self, msg):
        # all switch id coming from the handler zero based
        update_infos, finished, time_finishing_dependency_graph = self.handler.do_install_update(msg)
        self.log.debug(update_infos)
        for update_info in update_infos:
            assert update_info, P2PUpdateInfo
            self.__send_new_msgs__(update_info.msgs)

    # Handling all notification messages
    def handle_notification(self, msg):
        update_infos, finished = self.handler.do_handle_notification(msg)
        self.log.debug("update infos: %s" % str(update_infos))
        for update_info in update_infos:
            assert update_info, P2PUpdateInfo
            self.__send_new_msgs__(update_info.msgs)

    def __send_new_msgs__(self, new_msgs):
        for new_msg in new_msgs:
            self.send_to_switch(new_msg, new_msg.dst_id)

    def callback_func(self, update_info, finished):
        self.__send_new_msgs__(update_info.msgs)
