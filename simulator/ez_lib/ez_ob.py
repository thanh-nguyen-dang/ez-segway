import misc.constants as constants
import sys
from collections import defaultdict


class P2PUpdateInfo(object):
    def __init__(self, flow_id_, init_sw_, end_sw_, msgs_, update_next_=None, update_policy_=None):
        self.flow_id = flow_id_
        self.init_sw = init_sw_
        self.end_sw = end_sw_
        self.msgs = msgs_
        self.update_next_sw = update_next_
        self.update_policy = update_policy_

    def __str__(self):
        return "Update info: flow %d %d->%d, %s, %s"\
               % (self.flow_id, self.init_sw, self.end_sw, self.update_next_sw, self.msgs)

    def __repr__(self):
        return self.__str__()


class UpdateNext(object):
    def __init__(self, seg_path_id_, next_, type_):
        self.seg_path_id = seg_path_id_
        if type_ != constants.NO_UPDATE_NEXT:
            self.next_sw = next_
        else:
            self.next_sw = "No update"
        self.type = type_

    def __str__(self):
        return "next=%s, type=%s"\
               % (str(self.next_sw), self.__type_str__())

    def __repr__(self):
        return self.__str__()

    def __type_str__(self):
        if self.type == constants.UPDATE_NEXT:
            return "UPDATE_NEXT"
        if self.type == constants.ADD_NEXT:
            return "ADD_NEXT"
        if self.type == constants.REMOVE_NEXT:
            return "REMOVE_NEXT"
        if self.type == constants.NO_UPDATE_NEXT:
            return "NO_UPDATE_NEXT"
        return "NO_TYPE"


class UpdatePolicy(object):
    def __init__(self, flow_id_, init_sw_, end_sw_, policy_, type_):
        self.flow_id = flow_id_
        self.init_sw = init_sw_
        self.end_sw = end_sw_
        self.policy = policy_
        self.type = type_


class CenUpdateInfo(object):
    def __init__(self, seg_path_id_, init_sw_, end_sw_):
        self.seg_path_id = seg_path_id_
        self.init_sw = init_sw_
        self.end_sw = end_sw_
        self.update_nexts = {}
        self.update_policies = {}

    def __str__(self):
        return "Update info: flow %s %d->%d, %s"\
               % (self.seg_path_id, self.init_sw, self.end_sw, self.update_nexts)

    def __repr__(self):
        return self.__str__()


class UpdateOperation(object):
    # The reason to have this class is:
    # every switch only controls the traffic go in/out a link
    # this information is according to a given link
    # the direction of flow is indicated by the direction from:
    # (1) either smaller id switch to greater id switch
    # (2) or greater id switch to smaller id switch
    def __init__(self, seg_path_id, allocated_vol, related_free_vol_from_loop=0):
        self.seg_path_id = seg_path_id
        self.allocated_vol = allocated_vol
        self.related_free_vol_from_loop = related_free_vol_from_loop

    def __eq__(self, other):
        return self.seg_path_id == other.seg_path_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.allocated_vol < other.allocated_vol \
               or (self.allocated_vol == other.allocated_vol
                   and self.seg_path_id < other.seg_path_id)

    def __le__(self, other):
        return not other < self

    def __ge__(self, other):
        return not self < other

    def __str__(self):
        return "seg_path:%d.%d, #related_free_vol_from_loop=%d"\
               % (self.seg_path_id[0],self.seg_path_id[1], self.related_free_vol_from_loop)

    def __repr__(self):
        return self.__str__()


class ScheduleLinkToLink(object):
    def __init__(self, ops, hops=sys.maxint, vol_diff=0):
        self.ops = ops
        self.hop_distance = hops
        self.total_received_cap = 0
        self.total_released_cap = 0
        self.necessary_cap = 0
        self.unnecessary_cap = 0
        self.received_cap = 0
        self.received_ops = set()
        self.released_cap = 0
        self.released_ops = set()
        self.calculated_related_vol = False

    def __str__(self):
        return "ops:%s, hops=%d, total_receive_cap=%f, total_released_cap=%f, " \
               "nec=%f, unnec=%f, received=%f, received_ops=%s, " \
               "released=%f, released_op=%s"\
               % (self.ops, self.hop_distance, self.total_received_cap, self.total_released_cap,
                  self.necessary_cap, self.unnecessary_cap, self.received_cap, self.received_ops,
                  self.released_cap, self.released_ops)

    def __repr__(self):
        return self.__str__()
