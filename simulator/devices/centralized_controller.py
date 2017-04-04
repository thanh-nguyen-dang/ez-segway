from devices.controller import Controller
from domain.message import NotificationMessage
from ez_lib.cen_ctrl_handler import CenCtrlHandler
from ez_lib.ez_ob import CenUpdateInfo, UpdateNext
from misc import constants, global_vars
from time import time

class CentralizedController(Controller):
    def __init__(self):
        super(CentralizedController, self).__init__()
        self.handler = CenCtrlHandler(self.log, global_vars.switches)
        self.current_update = -1

    def __str__(self):
        return "Centralized Controller"

    def install_update(self, old_flows, new_flows):
        update_infos, time_finishing_dependency_graph = self.handler.do_install_update(old_flows, new_flows)
        self.send_messages(update_infos)

    def handle_notification(self, msg):
        update_infos = self.handler.do_handle_feedback(msg)
        self.send_messages(update_infos)

    def send_messages(self, update_infos):
        # It is only for simulation
        # do differently for experiment
        for key in update_infos.keys():
            update_info = update_infos[key]
            self.log.debug("Process update info: %s" % update_info)
            assert update_info, CenUpdateInfo
            for sw in update_infos[key].update_nexts.keys():
                update_next = update_info.update_nexts[sw]
                assert update_next, UpdateNext
                if update_next.type == constants.ADD_NEXT or update_next.type == constants.UPDATE_NEXT:
                    msg = NotificationMessage(0, sw, constants.ADDING_MSG, key, self.current_update, time() * 1000)
                    self.send_to_switch(msg, sw)
                elif update_next.type == constants.REMOVE_NEXT:
                    msg = NotificationMessage(0, sw, constants.REMOVING_MSG, key, self.current_update, time() * 1000)
                    self.send_to_switch(msg, sw)
