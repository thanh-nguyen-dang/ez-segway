from misc import constants
from time import time


class Message:
    def __init__(self, src_id, dst_id, update_id):
        self.src_id = src_id
        self.dst_id = dst_id
        self.update_id = update_id
        self.sending_time = None
        self.receiving_time = None

    def __str__(self):
        return "Message"

class AggregatedMessage(Message):
    def __init__(self, src_id, dst_id, msg_type, seg_path_ids, update_id):
        Message.__init__(self, src_id, dst_id, update_id)
        # self.msg_type = msg_type
        self.seg_path_ids = {}
        self.seg_path_ids[msg_type] = seg_path_ids

    def type_name(self, msg_type):
        if msg_type == constants.GOOD_TO_MOVE_MSG:
            return "GOOD_TO_MOVE"
        elif msg_type == constants.COHERENT_MSG:
            return "COHERENT"
        elif msg_type == constants.ALLOCATED_MSG:
            return "ALLOCATED"
        elif msg_type == constants.ADDING_MSG:
            return "ADDING"
        elif msg_type == constants.REMOVING_MSG:
            return "REMOVING"
        elif msg_type == constants.UPDATED_MSG:
            return "UPDATED"
        elif msg_type == constants.REMOVED_MSG:
            return "REMOVED"

    def __str__(self):
        str_type = ""
        for msg_type in self.seg_path_ids.keys():
            str_type += "%s: %s," % (self.type_name(msg_type), self.seg_path_ids[msg_type])

        return "msg: src=%s, dst=%s, %s"\
               % (self.src_id, self.dst_id, str_type)

    # def __repr__(self):
    #     return self.__str__()


class NotificationMessage(Message):
    def __init__(self, src_id, dst_id, msg_type, seg_path_id, update_id, sending_time=None, receiving_time=None):
        Message.__init__(self, src_id, dst_id, update_id)
        self.msg_type = msg_type
        self.seg_path_id = seg_path_id
        self.split_vol = -1
        if sending_time != None:
            self.sending_time = sending_time
        if receiving_time != None:
            self.receiving_time = receiving_time

    def __str__(self):
        msg_type = "what's type"
        if self.msg_type == constants.GOOD_TO_MOVE_MSG:
            msg_type = "GOOD_TO_MOVE"
        elif self.msg_type == constants.COHERENT_MSG:
            msg_type = "COHERENT"
        elif self.msg_type == constants.ALLOCATED_MSG:
            msg_type = "ALLOCATED"
        elif self.msg_type == constants.ADDING_MSG:
            msg_type = "ADDING"
        elif self.msg_type == constants.REMOVING_MSG:
            msg_type = "REMOVING"
        elif self.msg_type == constants.UPDATED_MSG:
            msg_type = "UPDATED"
        elif self.msg_type == constants.REMOVED_MSG:
            msg_type = "REMOVED"
        return "%s msg: src=%s, dst=%s, seg_path_id: %s, split_vol: %s, sending_time: %s, receiving_time: %s"\
               % (msg_type, self.src_id, self.dst_id, self.seg_path_id, self.split_vol, self.sending_time,
                  self.receiving_time)

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def serialize(obj):
        return {
            "msg_type": obj.msg_type,
            "src": obj.src_id,
            "dst": obj.dst_id,
            "seg_path_id": obj.seg_path_id,
            "split_vol": obj.split_vol
        }


class InstallUpdateMessage(Message):
    def __init__(self, src_id, dst_id, link_by_endpoints, segments_by_seg_path_id,
                 new_pred_by_flow,
                 new_succ_by_flow,
                 old_pred_by_flow,
                 old_succ_by_flow, update_id, skip_deadlock):
        Message.__init__(self, src_id, dst_id, update_id)
        self.link_by_endpoints = link_by_endpoints
        self.segments_by_seg_path_id = segments_by_seg_path_id
        self.new_pred_by_flow = new_pred_by_flow
        self.new_succ_by_flow = new_succ_by_flow
        self.old_pred_by_flow = old_pred_by_flow
        self.old_succ_by_flow = old_succ_by_flow
        self.skip_deadlock = skip_deadlock
        self.computation_time_in_ctrl = time() * 1000

    def __str__(self):
        return "Msg install update: src=%s, dst=%s, " \
               "link_by_endpoints=%s, segments_by_seg_path_id=%s, " \
               "update_time=%s, skip_deadlock=%s" % (
            self.src_id, self.dst_id, self.link_by_endpoints, self.segments_by_seg_path_id,
            self.update_id, self.skip_deadlock
        )
    def __repr__(self):
        return self.__str__()
