from misc import constants
from time import time


class Event:
    def __init__(self, type_, happening_sw_, happening_time_, text_=""):
        self.event_type = type_
        self.happening_sw = happening_sw_
        self.time = happening_time_
        self.text = text_
        self.dt = 1

    def __str__(self):
        return "event_type=%d, happening_sw=%s, happening_time=%s" \
               % (self.event_type, self.happening_sw, self.time)

    def __repr__(self):
        return self.__str__()

    def to_string(self):
        pass


class MessageSendingEvent(Event):
    def __init__(self, happening_sw_, dst_sw, happening_time_, msg_type_,
                 receiving_time_, seg_path_id_, split_vol_):
        Event.__init__(self, constants.SENDING_MSG_EVENT, happening_sw_, happening_time_)
        self.dst_sw = dst_sw
        self.receiving_time = receiving_time_
        self.seg_path_id = seg_path_id_
        self.msg_type = msg_type_
        self.split_vol = split_vol_

    def __str__(self):
        return "event_type=sendingMsg, msg_type=%d, src=%s, dst=%s, " \
               "sending_time=%s, receiving_time=%s, " \
               "seg_path_id=%s, split_vol=%s"\
               % (self.msg_type, self.happening_sw, self.dst_sw,
                  self.time, self.receiving_time,
                  self.seg_path_id, self.split_vol)

    def __repr__(self):
        return self.__str__()

    def to_string(self):
        (flow_id, seg_id) = self.seg_path_id
        if self.msg_type == constants.GOOD_TO_MOVE_MSG:
            return "GOOD_TO_MOVE"
        if self.msg_type == constants.REMOVING_MSG:
            return "REMOVING"
        if self.msg_type == constants.COHERENT_MSG:
            return "COHERENT"

class FlowChangeEvent(Event):
    def __init__(self, happening_sw_, happening_time_, seg_path_id_):
        Event.__init__(self, constants.CHANGING_FLOW_EVENT, happening_sw_, happening_time_)
        self.seg_path_id = seg_path_id_

    def __str__(self):
        return "event_type=FlowChange, happening_sw=%s, happening_time=%s, seg_path_id=%s" \
               % (self.happening_sw, self.time, self.seg_path_id)

    def __repr__(self):
        return self.__str__()

class FlowTableChangeEvent(Event):
    def __init__(self, happening_sw_, happening_time_, seg_path_id_, new_next_sw_, fl_tbl_type_):
        Event.__init__(self, constants.CHANGING_FLOW_EVENT, happening_sw_, happening_time_)
        self.seg_path_id = seg_path_id_
        self.next_sw = new_next_sw_
        self.fl_tbl_type = fl_tbl_type_

    def __str__(self):
        return "event_type=FlowTblEntryChange, happening_sw=%s, happening_time=%s, " \
               "seg_path_id=%s, next_sw=%d, type=%s" \
               % (self.happening_sw, self.time, self.seg_path_id,
                  self.next_sw, self.fl_tbl_type)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.seg_path_id == other.seg_path_id and self.time == other.time

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.time < other.time or \
            self.time == other.time and \
            ((self.seg_path_id[0] < other.seg_path_id[0])\
               or (self.seg_path_id[0] == other.seg_path_id[0] and self.seg_path_id[1] < other.seg_path_id[1]))

    def __le__(self, other):
        return not other < self

    def __ge__(self, other):
        return not self < other

