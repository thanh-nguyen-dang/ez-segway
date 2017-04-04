import constants


def get_item_with_lambda(sorted_seq, func):
    first = 0
    last = len(sorted_seq)-1
    found = False

    while first<=last:
        midpoint = (first + last)//2
        if func(sorted_seq[midpoint]) == 0:
            return sorted_seq[midpoint]
        else:
            if func(sorted_seq[midpoint]) < 0:
                last = midpoint-1
            else:
                first = midpoint+1
    return None


def is_flow_from_first_to_second(id1, id2, u_op):
    return (id1 < id2) == u_op.is_small_to_great


class ConjunctionPos(object):
    def __init__(self, nb_, pos1, pos2):
        self.nb = nb_
        self.pos1 = pos1
        self.pos2 = pos2
        self.selected_cycle = False

    def __str__(self):
        return "(%d: %d, %d)" % (self.nb, self.pos1, self.pos2)

    def __repr__(self):
        return "(%d: %d, %d)" % (self.nb, self.pos1, self.pos2)

    def __eq__(self, other):
        return self.nb == other.nb \
            and self.pos1 == other.pos1 \
            and self.pos2 == other.pos2

    def __ne__(self, other):
        return not self.__eq__(other)


class FlowSrcDst(object):
    def __init__(self, lt_id, gt_id, vol, reversed_vol):
        self.lt_id = lt_id if lt_id <= gt_id else gt_id
        self.gt_id = gt_id if lt_id <= gt_id else lt_id
        self.vol = vol
        self.reversed_vol = reversed_vol

    def __hash__(self):
        return hash((self.lt_id, self.gt_id))

    def __eq__(self, other):
        self_total = self.vol + self.reversed_vol
        other_total = other.vol + other.reversed_vol
        return (self_total == other_total) \
               and (self.lt_id, self.gt_id) == (other.lt_id, other.gt_id)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        self_total = self.vol + self.reversed_vol
        other_total = other.vol + other.reversed_vol
        return (self_total < other_total)\
               or (self_total < other_total and ((self.lt_id < other.lt_id) or (self.gt_id == other.gt_id)))

    def __le__(self, other):
        return not other < self

    def __ge__(self, other):
        return not self < other

    def __str__(self):
        return "(%d, %d): %s" % (self.lt_id, self.gt_id, self.vol)

    def __repr__(self):
        return self.__str__()


def get_flow_folder(data_dir, topology_type, generating_method, number_of_flows, failure_rate):
    if generating_method == constants.RANDOM_GENERATION:
        return "%s/%s/%s" % (data_dir, generating_method, \
                             str(number_of_flows))
    elif generating_method == constants.LINK_FAILURE_GENERATION:
        return "%s/%s/%s/%s" % (data_dir, generating_method, \
                                str(number_of_flows), str(failure_rate))
    elif generating_method == constants.MANUAL_GENERATION:
        return data_dir
