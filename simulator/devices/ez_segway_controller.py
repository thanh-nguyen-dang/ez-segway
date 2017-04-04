from devices.controller import *
from ez_lib import ez_ctrl_handler


class P2PController(Controller):
    def __init__(self):
        super(P2PController, self).__init__()
        self.handler = ez_ctrl_handler.EzCtrlHandler()

    def __str__(self):
        return "P2P Controller"

    def install_update(self, old_flows, new_flows):
        new_msgs = self.handler.do_install_update(old_flows, new_flows, self.current_update, self.skip_deadlock)
        for new_msg in new_msgs:
            self.send_to_switch(new_msg, new_msg.dst_id)
