from misc import constants
from collections import defaultdict

class GenFlow(object):
    def __init__(self, flow_id, src, dst, vol, update_type = constants.UPDATING_FLOW, reversed_vol = 0):
        self.flow_id = flow_id
        self.src = src
        self.dst = dst
        self._vol = vol
        self._reversed_vol = reversed_vol
        self.update_type = update_type

    @property
    def vol(self):
        return self._vol

    @vol.setter
    def vol(self, value):
        self._vol = value

    @property
    def reversed_vol(self):
        return self._reversed_vol

    @reversed_vol.setter
    def reversed_vol(self, value):
        self._reversed_vol = value

    def __eq__(self, other):
        return self.flow_id == other.flow_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.flow_id < other.flow_id

    def __le__(self, other):
        return not other < self

    def __ge__(self, other):
        return not self < other

    def __update_type_str__(self):
        if self.update_type == constants.UPDATING_FLOW:
            return "UPDATING_FLOW"
        if self.update_type == constants.REMOVING_FLOW:
            return "REMOVING_FLOW"
        if self.update_type == constants.ADDING_FLOW:
            return "ADDING_FLOW"
        return "NO_TYPE"


class GenSingleFlow(GenFlow):
    def __init__(self, flow_id, src, dst, vol, update_type = constants.UPDATING_FLOW, reversed_vol = 0):
        super(GenSingleFlow, self).__init__(flow_id, src, dst, vol, update_type, reversed_vol)
        self.path = []
        self.mdbxes = []
        self.skip_mdbxes = []
        self.segments = []

    def __str__(self):
        return "Flow%d: src=%d, dst=%d, paths=%s, vol=%f, reversed_vol=%f, update_type=%s" \
               % (self.flow_id, self.src, self.dst, str(self.path),
                  self.vol, self.reversed_vol, self.__update_type_str__())

    def __repr__(self):
        return self.__str__()

class GenMulFlow(GenFlow):
    def __init__(self, flow_id, src, dst, vol, update_type = constants.UPDATING_FLOW, reversed_vol = 0):
        super(GenMulFlow, self).__init__(flow_id, src, dst, vol, update_type, reversed_vol)
        self.path = []
        self.mdbxes = []
        self.skip_mdbxes = []
        self.segments = []
        self.unit_vol = float(vol)/constants.NO_MULT_PATH
        self.unit_reversed_vol = float(reversed_vol) / constants.NO_MULT_PATH

    @property
    def vol(self):
        return self._vol

    @vol.setter
    def vol(self, value):
        # count = self.non_empty_path_count
        # if count > 0:
        #     self.unit_vol = value / count
        self._vol = value

    @property
    def reversed_vol(self):
        return self._reversed_vol

    @reversed_vol.setter
    def reversed_vol(self, value):
        # count = self.non_empty_path_count
        # if count > 0:
        #     self.unit_reversed_vol = value / count
        self._reversed_vol = value

    @property
    def non_empty_path_count(self):
        count = 0
        for p in self.path:
            if p:
                count+=1
        return count

    def __str__(self):
        return "Flow%d: src=%d, dst=%d, paths=%s, vol=%f, reversed_vol=%f, update_type=%s" \
               % (self.flow_id, self.src, self.dst, str(self.path),
                  self.vol, self.reversed_vol, self.__update_type_str__())

    def __repr__(self):
        return self.__str__()

class Flow(object):
    def __init__(self, flow_id, src, dst, vol, update_type = constants.UPDATING_FLOW, reversed_vol = 0):
        self.flow_id = flow_id
        self.src = src
        self.dst = dst
        self.vol = vol
        self.reversed_vol = reversed_vol
        self.path = []
        self.update_type = update_type
        #self.last_policy = None

    @property
    def vol(self):
        return self._vol

    @vol.setter
    def vol(self, value):
        self._vol = value

    def __eq__(self, other):
        return self.flow_id == other.flow_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.flow_id < other.flow_id

    def __le__(self, other):
        return not other < self

    def __ge__(self, other):
        return not self < other

    def __update_type_str__(self):
        if self.update_type == constants.UPDATING_FLOW:
            return "UPDATING_FLOW"
        if self.update_type == constants.REMOVING_FLOW:
            return "REMOVING_FLOW"
        if self.update_type == constants.ADDING_FLOW:
            return "ADDING_FLOW"
        return "NO_TYPE"

    def __str__(self):
        return "Flow%d: src=%d, dst=%d, paths=%s, vol=%f, update_type=%s\n" \
               % (self.flow_id, self.src, self.dst, str(self.path), self.vol, self.__update_type_str__())

    def __repr__(self):
        return "Flow%d: src=%d, dst=%d, paths=%s, vol=%f, update_type=%s\n" \
               % (self.flow_id, self.src, self.dst, str(self.path), self.vol, self.__update_type_str__())

class Segment(object):
    def __init__(self, path_id, seg_id, init_sw, end_sw_new, end_sw_old, old_seg, new_seg, loop_pos):
        self.seg_path_id = (path_id, seg_id)
        self.init_sw = init_sw
        self.end_sw_new = end_sw_new
        self.end_sw_old = end_sw_old
        self.old_seg = old_seg
        self.new_seg = new_seg
        self.loop_pos = loop_pos

    def __eq__(self, other):
        if other is None:
            return False
        return self.seg_path_id == other.seg_path_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return (self.seg_path_id[0] < other.seg_path_id[0])\
               or (self.seg_path_id[0] == other.seg_path_id[0] and self.seg_path_id[1] < other.seg_path_id[1])

    def __le__(self, other):
        return not other < self

    def __ge__(self, other):
        return not self < other

    def __str__(self):
        return "segment: id=%d.%d,init_sw=%d,end_sw_new=%d,old_seg=%s,new_seg=%s;"\
               % (self.seg_path_id[0], self.seg_path_id[1], self.init_sw, self.end_sw_new, self.old_seg, self.new_seg)

    def __repr__(self):
        return "segment: id=%d.%d,init_sw=%d,end_sw_new=%d,old_seg=%s,new_seg=%s;"\
               % (self.seg_path_id[0], self.seg_path_id[1], self.init_sw, self.end_sw_new, self.old_seg, self.new_seg)


class Link(object):
    def __init__(self, pair, cap, to_removes, to_adds):
        (src, dst) = pair
        self.src = src
        self.dst = dst
        self.cap = cap
        self.to_removes = to_removes
        self.to_adds = to_adds
        self.to_adds_loop = []
        self.to_adds_only = []
        self.avail_cap = cap
        # self.links_from_adds = {}
        # self.links_from_removes = {}
        # self.created_links_from_adds = False
        # self.created_links_from_removes = False
        # self.distance_from_root = False
        # self.necessary_additional_cap = 0
        # self.unnecessary_additional_cap = 0
        # self.calculated_necessary_cap = False
        self.released_cap = 0
        self.required_cap = 0
        self.scheduling_mode = constants.NORMAL_LINK

    def __eq__(self, other):
        return self.src == other.src and self.dst == other.dst

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "link:%d-->%d, avail: %f, to_adds_only: %s, to_adds: %s, " \
               "to_adds_loop: %s, to_removes:%s" \
               % (self.src, self.dst, self.avail_cap, self.to_adds_only, self.to_adds,
                  self.to_adds_loop, self.to_removes)#, self.links_from_adds) #"rems:%s, adds:%s" % (self.exists, self.to_adds)

    def __repr__(self):
        return self.__str__()


class LinkSegment(object):
    def __init__(self, seg_path_id, init_sw, end_sw_new, end_sw_old, old_link_seg,
                 new_link_seg, flow_src, flow_dst, vol, loop_pos, remove_only_,
                 wait_for_ops=None, prioritized = False):
        self.seg_path_id = seg_path_id
        self.init_sw = init_sw
        self.end_sw_new = end_sw_new
        self.end_sw_old = end_sw_old
        self.flow_src = flow_src
        self.flow_dst = flow_dst
        self.old_link_seg = old_link_seg
        self.new_link_seg = new_link_seg
        self.old_seg = []
        self.new_seg = []
        self.vol = vol
        self.loop_pos = loop_pos
        self.update_status = constants.NOTHING
        self.is_splitting = False
        self.remove_only = remove_only_
        self.finished = False
        # if wait_for_ops is None:
        #     self.wait_for_segments = set()
        # else:
        #     self.wait_for_segments = wait_for_ops
        # self.prioritized = prioritized
        self.type_of_update = 0

    def __eq__(self, other):
        return self.seg_path_id == other.seg_path_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return (self.seg_path_id[0] < other.seg_path_id[0])\
               or (self.seg_path_id[0] == other.seg_path_id[0] and self.seg_path_id[1] < other.seg_path_id[1])

    def __le__(self, other):
        return not other < self

    def __ge__(self, other):
        return not self < other

    def __str__(self):
        loop_pos = "NONE_SEGMENT_LOOP"
        if self.loop_pos == constants.PRED_SEGMENT_LOOP_POS:
            loop_pos = "PRED_SEGMENT_LOOP"
        elif self.loop_pos == constants.OLD_IN_SEGMENT_LOOP_POS:
            loop_pos = "OLD_SEGMENT_IN_LOOP"
        elif self.loop_pos == constants.NEW_IN_SEGMENT_LOOP_POS:
            loop_pos = "NEW_IN_SEGMENT_LOOP_POS"

        return "segment: id=%d:%d,init_sw=%d,end_sw_new=%d,end_sw_old=%d," \
               "flow_src=%d,flow_dst=%d," \
               "old_link_seg=%s,new_link_seg=%s," \
               "old_seg=%s,new_seg=%s,vol=%f,loop_pos=%s;"\
               "remove_only=%s;"\
               % (self.seg_path_id[0], self.seg_path_id[1], self.init_sw, self.end_sw_new,
                  self.end_sw_old, self.flow_src, self.flow_dst,
                  self.old_link_seg, self.new_link_seg, self.old_seg, self.new_seg, self.vol,
                  loop_pos, self.remove_only)#, self.wait_for_segments)

    def __repr__(self):
        return self.__str__()

class NetworkUpdateInfo:
    def __init__(self):
        self.min_old_utilizing = 0
        self.max_old_utilizing = 0
        self.avg_old_utilizing = 0
        self.free_old_link = 0

        self.min_new_utilizing = 0
        self.max_new_utilizing = 0
        self.avg_new_utilizing = 0
        self.free_new_link = 0
        self.no_of_segments_by_count = {}

    def set_statistic_info_from_string(self, str_info):
        str_numbers = str_info.split('\t')
        self.min_old_utilizing = float(str_numbers[0])
        self.max_old_utilizing = float(str_numbers[1])
        self.avg_old_utilizing = float(str_numbers[2])
        self.free_old_link = int(str_numbers[3])
        self.min_new_utilizing = float(str_numbers[4])
        self.max_new_utilizing = float(str_numbers[5])
        self.avg_new_utilizing = float(str_numbers[6])
        self.free_new_link = float(str_numbers[7])

        i = 8
        while i < len(str_numbers):
            no_segments_str = str_numbers[i]
            str_splits = no_segments_str.split(':')
            self.no_of_segments_by_count[int(str_splits[0])] = int(str_splits[1])
            i += 1


class NetworkUpdate:
    def __init__(self, old_flows, new_flows):
        self.old_flows = old_flows
        self.new_flows = new_flows
        self.stat_info = NetworkUpdateInfo()

    def set_statistic_info_from_string(self, str_info):
        self.stat_info.set_statistic_info_from_string(str_info)
