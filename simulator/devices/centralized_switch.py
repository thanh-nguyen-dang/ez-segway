from devices.switch import *

class CentralizedSwitch(Switch):
    def __init__(self, id_, ctrl, neighbor_ids):
        super(CentralizedSwitch, self).__init__(id_, ctrl, neighbor_ids)

    def handle_notification(self, msg):
        if msg.msg_type == constants.ADDING_MSG:
            self.handle_adding_msg(msg)
        elif msg.msg_type == constants.REMOVING_MSG:
            self.handle_removing_msg(msg)
        elif msg.msg_type == constants.COHERENT_MSG:
            self.handle_coherent_msg(msg)

    def handle_adding_msg(self, msg):
        assert isinstance(msg, NotificationMessage)
        new_msg = NotificationMessage(self.id, global_vars.ctrl, constants.UPDATED_MSG,
                                      msg.seg_path_id, msg.update_id)
        self.send_to_ctrl(new_msg)

    def handle_removing_msg(self, msg):
        assert isinstance(msg, NotificationMessage)
        new_msg = NotificationMessage(self.id, global_vars.ctrl, constants.REMOVED_MSG,
                                      msg.seg_path_id, msg.update_id)
        self.send_to_ctrl(new_msg)

    def handle_coherent_msg(self, msg):
        new_msg = NotificationMessage(self.id, global_vars.ctrl, constants.COHERENT_MSG,
                                      0, msg.update_id)
        self.send_to_ctrl(new_msg)