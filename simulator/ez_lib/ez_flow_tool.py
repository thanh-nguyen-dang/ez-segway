import itertools
import collections
from collections import deque
from misc import logger, global_vars
from misc.utils import *
from domain.network_premitives import *
from ez_lib.ez_ob import *
from copy import deepcopy, copy

log = logger.getLogger("segmentation", constants.LOG_LEVEL)
PathDiffResult = collections.namedtuple("PathDiffResult", ["id", "to_same", "segs"])


def split_flows(old_flows, new_flows):
    log.debug("inside split_flows")
    current_id = len(old_flows) - 1
    additional_flows = []
    for old_flow, new_flow in itertools.izip(old_flows, new_flows):
        if old_flow.path == [] and new_flow.path != []:
            old_flow.update_type = constants.ADDING_FLOW
            new_flow.update_type = constants.ADDING_FLOW
        elif new_flow.path == [] and old_flow.path != []:
            old_flow.update_type = constants.REMOVING_FLOW
            new_flow.update_type = constants.REMOVING_FLOW
        elif old_flow.path != [] and new_flow.path != []:
            s_flow = __split_flow__(old_flow, new_flow, current_id)
            if s_flow is not None:
                current_id = s_flow.flow_id
                additional_flows.append(s_flow)

    for flow in additional_flows:
        if flow.update_type == constants.ADDING_FLOW:
            new_flows.append(flow)
            old_flows.append(Flow(flow.flow_id, flow.src, flow.dst, 0, constants.ADDING_FLOW))
        elif flow.update_type == constants.REMOVING_FLOW:
            new_flows.append(Flow(flow.flow_id, flow.src, flow.dst, 0, constants.REMOVING_FLOW))
            old_flows.append(flow)
    log.debug("end split_flows")


def __split_flow__(old_flow, new_flow, current_id):
    if old_flow.vol == new_flow.vol:
        return None
    elif old_flow.vol > new_flow.vol:
        delta_vol = old_flow.vol - new_flow.vol
        old_flow.vol -= delta_vol
        removing_flow = Flow(current_id + 1, old_flow.src, old_flow.dst, delta_vol, constants.REMOVING_FLOW)
        removing_flow.path = old_flow.path
        return removing_flow
    else:
        delta_vol = new_flow.vol - old_flow.vol
        new_flow.vol -= delta_vol
        adding_flow = Flow(current_id + 1, old_flow.src, old_flow.dst, delta_vol, constants.ADDING_FLOW)
        adding_flow.path = new_flow.path
        return adding_flow


def update_links_and_segments(psegs, links_by_endpoints, segments_by_seg_path_id,
                              old_flow, new_flow, centralized, demo=False):
    for seg in psegs:
        link_segment = LinkSegment(seg.seg_path_id, seg.init_sw, seg.end_sw_new, seg.end_sw_old,
                                   [], [], old_flow.src, old_flow.dst, old_flow.vol,
                                   seg.loop_pos, new_flow.path == [])
        if centralized:
            link_segment.new_seg = seg.new_seg
            link_segment.old_seg = seg.old_seg
        src = seg.init_sw
        for s_id in seg.new_seg:
            # every link is bidirectional and has unique end_points
            #   that is ascending ordered pair of (src, dest)
            link = update_link_cap(links_by_endpoints, (src, s_id), demo)

            # every link has the two list of update_operations
            #  that relevant to the link
            #   (1) to_adds, (2) to_removes
            #   every item in one of list has:
            #       - the path segment in which it belongs
            #       - the direction (from smaller to greater id or reversely)
            if new_flow.update_type == constants.UPDATING_FLOW:
                link.to_adds.append(UpdateOperation(seg.seg_path_id, new_flow.vol))
            if new_flow.update_type == constants.ADDING_FLOW:
                link.to_adds_only.append(UpdateOperation(seg.seg_path_id, new_flow.vol))
            link_segment.new_link_seg.append((src, s_id))
            src = s_id

        src = seg.init_sw
        for s_id in seg.old_seg:
            link = update_link_cap(links_by_endpoints, (src, s_id), demo)
            link.to_removes.append(UpdateOperation(seg.seg_path_id, new_flow.vol))
            link_segment.old_link_seg.append((src, s_id))
            # link.avail_cap -= old_flow.vol
            src = s_id
        segments_by_seg_path_id[seg.seg_path_id] = link_segment


def path_to_ops_by_link(id, links_by_endpoints, segments_by_seg_path_id, old_flow, new_flow, centralized=False,
                        do_segmenttation=True, demo=False):
    # path_d = path_diff(id, old_flow.path, new_flow.path)
    if len(old_flow.path) > 0 and len(new_flow.path) > 0:
        path_d = __path_segmentation__(id, old_flow.path, new_flow.path)

    else:
        if len(old_flow.path) == 0:
            segs = Segment(new_flow.flow_id, 0, new_flow.src, new_flow.dst,
                           old_flow.dst, [],
                           new_flow.path[1:len(new_flow.path)],
                           constants.NONE_SEGMENT_LOOP_POS)
        else:
            segs = Segment(old_flow.flow_id, 0, old_flow.src, old_flow.dst,
                           old_flow.dst, old_flow.path[1:len(old_flow.path)],
                           [],
                           constants.NONE_SEGMENT_LOOP_POS)
        path_d = PathDiffResult(new_flow.flow_id, [], [segs])

    if links_by_endpoints is not None and segments_by_seg_path_id is not None:
        update_links_and_segments(path_d.segs, links_by_endpoints, segments_by_seg_path_id, old_flow, new_flow,
                                  centralized, demo)

    #
    # if len(path_d.to_same) > 0:
    #     __update_link_capacity_with_to_same__(path_d, old_flow, links_by_endpoints)
    return path_d.to_same, len(path_d.segs)


def __is_staying_same__(old, new, sw, old_index):
    new_index = new.index(sw)
    old_pred = old[old_index - 1] if old_index - 1 >= 0 else None
    new_pred = new[new_index - 1] if new_index - 1 >= 0 else None
    old_succ = old[old_index + 1] if old_index + 1 < len(old) else None
    new_succ = new[new_index + 1] if new_index + 1 < len(new) else None

    pred = 1 if old_pred == new_pred else 0
    succ = 1 if old_succ == new_succ else 0
    return (pred << 1) | succ


def __find_non_overlapping_cycles__(covered, existing_set):
    """
    In every common switch, just pick up only one swap order pair of switches
    :param covered:
    :param existing_set:
    :return:
    """
    selected = set()
    removes = set()
    current_cycle = None
    for cv in covered:
        if len(cv) > 0:
            if current_cycle is None \
                    or (current_cycle is not None
                        and current_cycle not in cv):
                current_cycle = None
                for x in cv:
                    if (existing_set == []) or (x not in removes and x in existing_set):
                        current_cycle = x
                        break
                if current_cycle is not None:
                    selected.add(current_cycle)
            for i in cv:
                if current_cycle is None or i != current_cycle:
                    removes.add(i)
    return selected


def __find_maximal_non_overlapping_cycles__(longest_swap_order_pairs, length_of_common_sw):
    """
    This function will have the input as the list of swap order pairs of switches and the number of common switches
    the function will find the maximal non-overlapping of pairs that can cover largest number of switches.
    :param longest_swap_order_pairs: the list of swap-order pairs of switches
    :param length_of_common_sw: the number of common switches that appear in both old and new paths
    :return: the list of common switches that
    """
    # covered_old, covered_new are the two lists of common switches,
    # each item has the list of pair that contains common switches
    covered_old = []
    covered_new = []
    for i in xrange(length_of_common_sw):
        covered_old.append([])
        covered_new.append([])
    current_cycle = 0

    for (i, j) in longest_swap_order_pairs:
        id_old = i.pos1
        while id_old <= j.pos1:
            covered_old[id_old].append(current_cycle)
            id_old += 1
        id_new = j.pos2
        while id_new <= i.pos2:
            covered_new[id_new].append(current_cycle)
            id_new += 1
        current_cycle += 1

    log.debug("covered_old: %s, len=%d " % (covered_old, len(covered_old)))
    log.debug("covered_new: %s, len=%d " % (covered_new, len(covered_new)))
    selected_old = __find_non_overlapping_cycles__(covered_old, [])
    log.debug("selected_old: %s, len=%d " % (selected_old, len(selected_old)))
    selected_new = __find_non_overlapping_cycles__(covered_new, selected_old)
    log.debug("selected_new: %s, len=%d " % (selected_new, len(selected_new)))
    selected_swap_order_pairs = []

    for idx in selected_old & selected_new:
        selected_swap_order_pairs.append(longest_swap_order_pairs[idx])
    return selected_swap_order_pairs


def __find_cycle_segment__(id, old, new, common_sw_by_old):
    """
    :param id: id of path
    :param old: old path
    :param new: new path
    :param common_sw_by_old: common switches in both old and new path by ordered by the appearance in the old path
    :return: return the longest "swap ordered" pair
    """
    longest_swap_order_pairs = []
    changed_order_in_commons = []
    for common_obj in common_sw_by_old:
        if common_obj.pos1 != common_obj.pos2:
            changed_order_in_commons.append(common_obj.nb)

    max_diff = 0
    min_diff = 0
    for src in changed_order_in_commons[0:len(changed_order_in_commons) - 1]:
        i = (x for x in common_sw_by_old if x.nb == src).next()
        for dst in changed_order_in_commons[changed_order_in_commons.index(src) + 1:len(changed_order_in_commons)]:
            j = (x for x in common_sw_by_old if x.nb == dst).next()
            if (i.pos1 - j.pos1) * (i.pos2 - j.pos2) > 0:
                continue
            if longest_swap_order_pairs is [] \
                    or max_diff < max(abs(i.pos1 - j.pos1), abs(i.pos2 - j.pos2)) \
                    or (max_diff == max(abs(i.pos1 - j.pos1), abs(i.pos2 - j.pos2))
                        and min_diff < min(abs(i.pos1 - j.pos1), abs(i.pos2 - j.pos2))):
                longest_swap_order_pairs = [(i, j)]
                max_diff = max(abs(i.pos1 - j.pos1), abs(i.pos2 - j.pos2))
                min_diff = min(abs(i.pos1 - j.pos1), abs(i.pos2 - j.pos2))

            elif max_diff == max(abs(i.pos1 - j.pos1), abs(i.pos2 - j.pos2)) \
                    and min_diff == min(abs(i.pos1 - j.pos1), abs(i.pos2 - j.pos2)):
                longest_swap_order_pairs.append((i, j))

    log.debug("list of swap order pairs: %s" % longest_swap_order_pairs)
    if len(longest_swap_order_pairs) == 1:
        return longest_swap_order_pairs
    selected_swap_order_pairs = __find_maximal_non_overlapping_cycles__(longest_swap_order_pairs, len(common_sw_by_old))
    log.debug("list of selected swap order pairs: %s" % selected_swap_order_pairs)
    return selected_swap_order_pairs


def __get_next_selected_common_idx__(current_common_idx, common_list, cycle_edge):
    common_next_idx = current_common_idx + 1
    if not cycle_edge:
        while common_list[common_next_idx].pos1 != common_list[common_next_idx].pos2 \
                and not common_list[common_next_idx].selected_cycle:
            common_next_idx += 1
    else:
        while common_list[common_next_idx].pos1 == common_list[common_next_idx].pos2 \
                or not common_list[common_next_idx].selected_cycle:
            common_next_idx += 1
    return common_next_idx


def __path_diff_cycle__(id, old, new, common_sw_by_old, common_sw_by_new, has_cycle=False):
    """
    :param id: id of path
    :param old: old path
    :param new: new path
    :param common_sw_by_old: common switches in both old and new path by ordered by the appearance in the old path
    :param common_sw_by_new: common switches in both old and new path by ordered by the appearance in the new path
    :return: return the longest "swap ordered" pair
    """
    swap_order_pairs = []
    if has_cycle:
        log.debug("Flow id: %d, old: %s, new: %s" % (id, old, new))
        log.debug(common_sw_by_old)
        log.debug(common_sw_by_new)
        swap_order_pairs = __find_cycle_segment__(id, old, new, common_sw_by_old)
        for (obj1, obj2) in swap_order_pairs:
            common_sw_by_old[obj1.pos1].selected_cycle = True
            common_sw_by_old[obj2.pos1].selected_cycle = True
            common_sw_by_new[obj1.pos2].selected_cycle = True
            common_sw_by_new[obj2.pos2].selected_cycle = True

    segs = []
    to_same = []
    current_seg_id = 1
    cycle_index = 0
    common_idx = 0
    in_cycle = False
    max_common_idx_of_current_cycle = 0
    while common_idx < len(common_sw_by_old) - 1:
        common_old = common_sw_by_old[common_idx]
        old_index = old.index(common_old.nb)
        if common_old.pos1 == common_old.pos2:
            old_next_idx = __get_next_selected_common_idx__(common_idx, common_sw_by_old, False)
            old_next = common_sw_by_old[old_next_idx]
            assert (old_next_idx < len(common_sw_by_old))

            if old_next.pos1 == old_next.pos2:
                diff_result = __is_staying_same__(old, new, common_old.nb, old_index)
                if diff_result ^ 3 == 0:
                    to_same.append(common_old.nb)
                    if old_next_idx == len(common_sw_by_old) - 1:
                        to_same.append(old_next.nb)
                else:
                    # if diff_result | 1 == 1:  # old_pred != new_pred:
                    if diff_result | 2 == 2:  # old_succ != new_succ:
                        segs.append(Segment(id, current_seg_id, common_old.nb, old_next.nb, old_next.nb,
                                            old[(old_index + 1):(old.index(old_next.nb) + 1):],
                                            new[(new.index(common_old.nb) + 1):(new.index(old_next.nb) + 1)],
                                            constants.NONE_SEGMENT_LOOP_POS))
                        current_seg_id += 1
            else:
                (i, j) = swap_order_pairs[cycle_index]
                segs.append(Segment(id, current_seg_id, common_old.nb, j.nb, old_next.nb,
                                    old[(old_index + 1):(old.index(old_next.nb) + 1):],
                                    new[(new.index(common_old.nb) + 1):(new.index(j.nb) + 1)],
                                    constants.PRED_SEGMENT_LOOP_POS))
                max_common_idx_of_current_cycle = max(i.pos2, j.pos1)
                current_seg_id += 1
            common_idx = old_next_idx

        elif common_old.selected_cycle:
            in_cycle = not in_cycle
            old_next_idx = __get_next_selected_common_idx__(common_idx, common_sw_by_old, in_cycle)
            log.debug(max_common_idx_of_current_cycle)
            if not in_cycle:
                while old_next_idx <= max_common_idx_of_current_cycle:
                    old_next_idx = __get_next_selected_common_idx__(old_next_idx, common_sw_by_old, in_cycle)
            old_next = common_sw_by_old[old_next_idx]

            log.debug(common_old)
            new_next_idx = __get_next_selected_common_idx__(common_old.pos2, common_sw_by_new, not in_cycle)
            if in_cycle:
                while new_next_idx <= max_common_idx_of_current_cycle:
                    new_next_idx = __get_next_selected_common_idx__(new_next_idx, common_sw_by_old, not in_cycle)
            new_next = common_sw_by_new[new_next_idx]

            log.debug("old_next: %s, new_next: %s" % (old_next, new_next))
            loop_pos = constants.OLD_IN_SEGMENT_LOOP_POS if in_cycle \
                else constants.NEW_IN_SEGMENT_LOOP_POS
            segs.append(Segment(id, current_seg_id, common_old.nb, new_next.nb, old_next.nb,
                                old[(old_index + 1):(old.index(old_next.nb) + 1):],
                                new[(new.index(common_old.nb) + 1):(new.index(new_next.nb) + 1)],
                                loop_pos))
            common_idx = old_next_idx
            last_new_next_idx = new_next_idx
            current_seg_id += 1
    return PathDiffResult(id, to_same, segs)


def __path_segmentation__(id, old, new):
    """Diff two paths.
    id: id of the pair of paths
    old: list of old (switch, next) pairs
    new: lest of new (switch, next) pairs
    """
    common_sws_by_old, common_sws_by_new = __conjunction_switches__(old, new)
    has_cycle = False
    for con_pos in common_sws_by_old:
        if con_pos.pos1 != con_pos.pos2:
            has_cycle = True
            break
    if has_cycle:
        log.debug("Has cycle")
        return __path_diff_cycle__(id, old, new, common_sws_by_old, common_sws_by_new, True)
    else:
        # return path_diff(id, old, new)
        return __path_diff_cycle__(id, old, new, common_sws_by_old, common_sws_by_new)


def __conjunction_switches__(old, new):
    # traverse all switches that are in old and new path
    # to have the old and new positions according to the appearance of switches in the old path
    common_sws_by_old = []
    pos = 0
    for sw in old:
        if sw in new:
            common_sws_by_old.append(ConjunctionPos(sw, pos, -1))
            pos += 1

    # traverse all switches that are in old and new path
    # to have the old and new positions according to the appearance of switches in the new path
    common_sws_by_new = []
    pos = 0
    for sw in new:
        common_ob = next((x for x in common_sws_by_old if x.nb == sw), None)
        if common_ob is not None:
            common_ob.pos2 = pos
            common_sws_by_new.append(ConjunctionPos(sw, -1, pos))
            pos += 1

    # traverse all common switches to assign the old position in the common list
    # according to the appearance of switches in the new path
    for common_ob_old in common_sws_by_old:
        common_ob_new = next((x for x in common_sws_by_new if x.nb == common_ob_old.nb), None)
        if common_ob_new is not None:
            common_ob_new.pos1 = common_ob_old.pos1

    return common_sws_by_old, common_sws_by_new


def update_link_cap(links_by_endpoints, end_points, demo=False):
    if demo:
        link = Link(end_points, constants.DEFAULT_CAP, [], [])
        return link
    if end_points not in links_by_endpoints:
        link = Link(end_points, global_vars.link_capacities[end_points], [], [])
        links_by_endpoints[end_points] = link
    else:
        link = links_by_endpoints[end_points]
    return link


def compute_available_link_capacity(link_by_endpoints, old_flows, new_flows):
    for old_flow in old_flows:
        if len(old_flow.path) == 0:
            continue
        for src, dst in itertools.izip(old_flow.path[:len(old_flow.path) - 1], old_flow.path[1:len(old_flow.path)]):
            link = update_link_cap(link_by_endpoints, (src, dst))
            link.avail_cap -= old_flow.vol
            link.released_cap += old_flow.vol

    for new_flow in new_flows:
        if len(new_flow.path) == 0:
            continue
        for src, dst in itertools.izip(new_flow.path[:len(new_flow.path) - 1], new_flow.path[1:len(new_flow.path)]):
            link = update_link_cap(link_by_endpoints, (src, dst))
            link.required_cap += new_flow.vol

#    for endpoints in link_by_endpoints.keys():
#        link = link_by_endpoints[endpoints]
#        cap = link.avail_cap + link.released_cap - link.required_cap
#        print("Link {0} has current cap {1}, and final cap {2}".format(endpoints, link.avail_cap, cap))
#        if link.avail_cap < 0 or cap < 0:
#            raise Exception("Capacity of {0} is invalid: {1}".format(str(link), str(cap)))


def create_dependency_graph(old_flows, new_flows, links_by_endpoints,
                            segments_by_seg_path_id, to_sames=defaultdict(list), do_segmentation=True):
    split_flows(old_flows, new_flows)
    compute_available_link_capacity(links_by_endpoints, old_flows, new_flows)
    for old_flow, new_flow in itertools.izip(old_flows, new_flows):
        to_same, segment_length = path_to_ops_by_link(old_flow.flow_id, links_by_endpoints,
                                                      segments_by_seg_path_id, old_flow, new_flow, True)
        to_sames[old_flow.flow_id] = to_same


def has_deadlock(old_flows, new_flows):
    links_by_endpoints = defaultdict(Link)
    segments_by_seg_path_id = defaultdict(LinkSegment)
    create_dependency_graph(old_flows, new_flows,
                            links_by_endpoints, segments_by_seg_path_id)
    # log.debug("created dependency graph")
    for link in links_by_endpoints.values():
        has = find_deadlock_for_link(link, links_by_endpoints, segments_by_seg_path_id)
        # log.debug("Link %d-%d has deadlock = %s" % (link.src, link.dst, str(has)))
        if has:
            log.debug("Having deadlock")
            return True
    return False


def find_dependency_loop_for_directed_ops(link, list_ops, list_ops_loop, links_by_endpoint, segments_by_seg_path_id):
    list_op_loop_ids = []
    for u_op in list_ops:
        u_op_id = u_op.seg_path_id
        stack = [u_op_id]
        log.debug("START checking loop for link %d->%d, update_op %s" \
                  % (link.src, link.dst, u_op.seg_path_id))
        list_checked_segment_ids = set()
        end_op_ids = set()
        has_dependent = False
        while len(stack) > 0:
            log.debug("stack: %s" % stack)
            seg_path_id = stack.pop()
            is_dependent = traverse_op_for_loop(stack, segments_by_seg_path_id, links_by_endpoint,
                                                seg_path_id, link, end_op_ids,
                                                list_checked_segment_ids)
            if is_dependent:
                has_dependent = True

        log.debug("end_op_ids: %s" % end_op_ids)
        if len(end_op_ids) > 0 and has_dependent:
            # compute the released volume
            released_vol = 0
            for op_id in end_op_ids:
                released_vol += segments_by_seg_path_id[op_id].vol
            list_ops_loop.append(UpdateOperation(u_op_id, segments_by_seg_path_id[u_op_id].vol, released_vol))
            list_op_loop_ids.append(u_op_id)

    return list_op_loop_ids


#  This function needs to be refactored:
#  The objective of this function is to find all the critical cycle (to_adds_loop)
def find_dependency_loop_for_link(link, links_by_endpoint, segments_by_seg_path_id):
    list_op_loop_ids = []
    list_checked_segment_ids = {}
    deadlock = False
    for u_op in link.to_adds:
        assert isinstance(u_op, UpdateOperation)
        queue = [u_op]
        log.debug("start checking loop for link %d->%d, update_op %s" \
                  % (link.src, link.dst, u_op.seg_path_id))
        end_op_ids = set()
        total_end_op_vol = 0

        while len(queue) > 0:
            log.debug("queue: %s" % queue)
            curr_item = queue.pop()
            stop = traverse_op_for_deadlock(queue, segments_by_seg_path_id,
                                            links_by_endpoint,
                                            curr_item.seg_path_id, link, u_op, end_op_ids,
                                            list_checked_segment_ids, total_end_op_vol, True)
            if stop:
                break

        # debug("final count=%d, final count_loop=%d" % (count_op, count_dependency_loop))
        log.debug("end_op_ids: %s" % end_op_ids)
        if len(end_op_ids) > 0:
            released_vol = 0
            for op_id in end_op_ids:
                released_vol += segments_by_seg_path_id[op_id].vol
            link.to_adds_loop.append(UpdateOperation(u_op.seg_path_id,
                                                     segments_by_seg_path_id[u_op.seg_path_id].vol,
                                                     released_vol))
            list_op_loop_ids.append(u_op.seg_path_id)

    link.to_adds[:] = [x for x in link.to_adds if x.seg_path_id not in list_op_loop_ids]
    link.to_adds_loop = list(reversed(sorted(link.to_adds_loop)))
    link.to_adds = list(reversed(sorted(link.to_adds)))


#  This function does:
#  - Finding all dependency loop for every outgoing link from a switch
#  - Prioritizing the update for every update operation
def prioritizing_update_segments(local_links, links_by_endpoint, segments_by_seg_path_id):
    for link in local_links.values():
        find_dependency_loop_for_link(link, links_by_endpoint, segments_by_seg_path_id)

    # for link in local_links.values():
    #     compute_scheduling_info_for_a_link(link, links_by_endpoint, segments_by_seg_path_id)


def calculate_necessary_cap_for_link(link, segments_by_seg_path_id):
    if link.calculated_necessary_cap:
        return
    adding_vol = 0
    for add_op in link.to_adds + link.to_adds_loop + link.to_adds_only:
        adding_vol += segments_by_seg_path_id[add_op.seg_path_id].vol
    link.necessary_additional_cap = max(adding_vol - link.avail_cap, 0)
    link.unnecessary_additional_cap = max(adding_vol - link.necessary_additional_cap, 0)


#  link: link that need to compute the difference
#  link_next_to_root: the link that next to the update operation node
#  Compute the difference of total volume carried by a link in two configurations
def calculate_vol_diff(link, link_next_to_root, forwarded_link, segments_by_seg_path_id):
    schedule_link_to_link = link.links_from_adds[(link_next_to_root.src, link_next_to_root.dst)]
    schedule_link_to_link_2 = forwarded_link.links_from_adds[(link.src, link.dst)]
    vol_diff = 0
    for op in schedule_link_to_link.ops:
        schedule_link_to_link.total_released_cap += segments_by_seg_path_id[op.seg_path_id].vol
    for op in schedule_link_to_link_2.ops:
        schedule_link_to_link.total_received_cap += segments_by_seg_path_id[op.seg_path_id].vol
    log.debug("schedule_link_to_link obj corresponding to %d->%d on link %d->%d: %s"
              % (link_next_to_root.src, link_next_to_root.dst, link.src, link.dst,
                 schedule_link_to_link))
    log.debug("schedule_link_to_link_2 obj corresponding to %d->%d on link %d->%d: %s"
              % (link.src, link.dst, forwarded_link.src, forwarded_link.dst,
                 schedule_link_to_link_2,))

    calculate_necessary_cap_for_link(link_next_to_root, segments_by_seg_path_id)
    schedule_link_to_link.necessary_cap = link_next_to_root.necessary_additional_cap
    schedule_link_to_link.unnecessary_cap = link_next_to_root.unnecessary_additional_cap
    schedule_link_to_link.calculated_related_vol = True


# Now, only use to check deadlock in generated update
def compute_scheduling_info_for_a_link(link, links_by_endpoint, segments_by_seg_path_id):
    distance_from_root = 1
    distance_from_root_by_link = {}
    links_next_to_roots = {}
    log.debug("scheduling for link: %s" % link)
    create_links_from_adds(link, links_by_endpoint, segments_by_seg_path_id,
                           distance_from_root_by_link, links_next_to_roots, distance_from_root)
    queue = deque(link.links_from_adds.keys())
    log.debug("queue for link %d->%d: %s" % (link.src, link.dst, queue))
    traversed = set(queue)

    while len(queue) > 0:
        lnk_pair = queue.popleft()
        forwarded_link = links_by_endpoint[lnk_pair]
        log.debug("forwarded link: %d->%d, distance_from_root: %d"
                  % (forwarded_link.src, forwarded_link.dst, distance_from_root_by_link[lnk_pair]))
        distance_from_root = distance_from_root_by_link[lnk_pair] + 1
        if distance_from_root > 3:
            continue
        create_links_from_adds(forwarded_link, links_by_endpoint, segments_by_seg_path_id,
                               distance_from_root_by_link, links_next_to_roots, distance_from_root)
        # log.debug("distance from root by link: %s" % str(distance_from_root_by_link))
        # log.debug("links_next_to_roots: %s" % links_next_to_roots)

        if (link.src, link.dst) not in forwarded_link.links_from_adds.keys():
            for pair in forwarded_link.links_from_adds:
                if pair not in traversed:
                    queue.append(pair)
                    traversed.add(pair)
        else:
            if distance_from_root_by_link[lnk_pair] > 1:
                for pair_next_to_root in links_next_to_roots[lnk_pair]:
                    if link.links_from_adds[pair_next_to_root].hop_distance > distance_from_root_by_link[lnk_pair]:
                        link.links_from_adds[pair_next_to_root].hop_distance = distance_from_root_by_link[lnk_pair]
                        link_next_to_root = links_by_endpoint[pair_next_to_root]
                        calculate_vol_diff(link, link_next_to_root, forwarded_link, segments_by_seg_path_id)
            else:
                link.links_from_adds[lnk_pair].hop_distance = distance_from_root_by_link[lnk_pair]
                calculate_vol_diff(link, forwarded_link, forwarded_link, segments_by_seg_path_id)
        log.debug("queue for link %d->%d: %s" % (link.src, link.dst, queue))

    for link in links_by_endpoint.values():
        link.links_next_to_root = set()
        link.distance_from_root = 0


#  create link from all 'to_add' and 'to_add_loop' update operations related to a link
def create_links_from_adds(link, links_by_endpoint, segments_by_seg_path_id,
                           distance_from_root_by_link, links_next_to_roots, distance_from_root):
    for u_op in link.to_adds + link.to_adds_loop:
        segment = segments_by_seg_path_id[u_op.seg_path_id]
        for pair in segment.old_link_seg:
            if not link.created_links_from_adds:
                if not link.links_from_adds.has_key(pair):
                    link.links_from_adds[pair] = ScheduleLinkToLink(set())
                link.links_from_adds[pair].ops.add(u_op)

            if not distance_from_root_by_link.has_key(pair):
                distance_from_root_by_link[pair] = distance_from_root
                # log.debug("distance from root by link %s: %d" % (pair, distance_from_root))
            link_pair = (link.src, link.dst)
            if distance_from_root == 2:
                lst = [link_pair]
                if not links_next_to_roots.has_key(pair):
                    links_next_to_roots[pair] = set()
                links_next_to_roots[pair] |= set(lst)
            elif distance_from_root > 2:
                if not links_next_to_roots.has_key(pair):
                    links_next_to_roots[pair] = set()
                if links_next_to_roots.has_key(link_pair):
                    links_next_to_roots[pair] |= links_next_to_roots[link_pair]


def prioritize_loop_for_link(link, links_by_endpoint, segments_by_seg_path_id, to_check_deadlock=False):
    total_required_vol_except = {}
    total_required_vol = get_total_required_vol_except(segments_by_seg_path_id, link, None)
    # create_links_from_add_and_remove(link, links_by_endpoint, segments_by_seg_path_id)
    compute_scheduling_info_for_a_link(link, links_by_endpoint, segments_by_seg_path_id)
    for u_op in link.to_adds:
        total_required_vol_except[u_op.seg_path_id] = get_total_required_vol_except(segments_by_seg_path_id,
                                                                                    link, u_op.seg_path_id)

    for lnk_pair in link.links_from_adds.keys():
        if link.links_from_removes.has_key(lnk_pair):
            prior_ops = set()
            required_vol_2 = get_total_required_vol_except(segments_by_seg_path_id,
                                                           links_by_endpoint[lnk_pair], None)
            for u_op in link.links_from_adds[lnk_pair].ops:
                total_avail_except_2 = get_total_availability_except(segments_by_seg_path_id,
                                                                     links_by_endpoint[lnk_pair],
                                                                     u_op.seg_path_id)
                if total_avail_except_2 < required_vol_2:
                    for op in link.links_from_removes[lnk_pair].ops:
                        total_avail_except_1 = get_total_availability_except(segments_by_seg_path_id,
                                                                             link,
                                                                             op.seg_path_id)
                        if total_required_vol > total_avail_except_1 > total_required_vol_except[u_op.seg_path_id]:
                            prior_ops.add(u_op.seg_path_id)
                            segments_by_seg_path_id[u_op.seg_path_id].prioritized = True
                            break

            for u_op in link.links_from_adds[lnk_pair].ops:
                if u_op.seg_path_id not in prior_ops:
                    segments_by_seg_path_id[u_op.seg_path_id].wait_for_segments |= prior_ops

            if len(prior_ops) > 0 and to_check_deadlock:
                free_avail = links_by_endpoint[lnk_pair].avail_cap
                for removed_op in links_by_endpoint[lnk_pair].to_removes:
                    segment = segments_by_seg_path_id[removed_op.seg_path_id]
                    if not segment.prioritized and len(segment.wait_for_segments) == 0:
                        free_avail += segment.vol
                for common_add_op in link.links_from_removes[lnk_pair].ops:
                    free_avail -= segments_by_seg_path_id[common_add_op.seg_path_id].vol
                if free_avail < 0:
                    return False
    return True


def find_deadlock_for_link(link, links_by_endpoint, segments_by_seg_path_id):
    list_checked_segment_ids = {}
    for u_op in link.to_adds:
        assert isinstance(u_op, UpdateOperation)
        queue = [u_op]
        log.debug("START checking deadlock for link %d->%d, update_op %s" \
                  % (link.src, link.dst, u_op.seg_path_id))
        end_op_ids = set()
        total_end_op_vol = 0
        while len(queue) > 0:
            log.debug("queue: %s" % queue)
            curr_item = queue.pop()
            deadlock = traverse_op_for_deadlock(queue, segments_by_seg_path_id,
                                                links_by_endpoint,
                                                curr_item.seg_path_id, link, u_op, end_op_ids,
                                                list_checked_segment_ids, total_end_op_vol)
            if deadlock:
                return True

        # debug("final count=%d, final count_loop=%d" % (count_op, count_dependency_loop))
        # log.debug("end_op_ids: %s" % end_op_ids)
        # if len(end_op_ids) > 0:
        #     link.to_adds_loop.append(u_op)
        #     all_avail_cap = link.avail_cap
        #     for rm_op in link.to_removes:
        #         if rm_op.seg_path_id not in end_op_ids:
        #             all_avail_cap += segments_by_seg_path_id[rm_op.seg_path_id].vol
        #     if all_avail_cap < segments_by_seg_path_id[u_op.seg_path_id].vol:
        #         deadlock = True
        #         return deadlock
        return False

    return False #not prioritize_loop_for_link(link, links_by_endpoint, segments_by_seg_path_id, True)


def traverse_op_for_loop(queue, segments_by_seg_path_id, links_by_endpoint,
                         segment_id, origin_link, end_op_ids, list_checked_segment_ids):
    is_dependent_to_loop = False
    for pair in segments_by_seg_path_id[segment_id].old_link_seg:
        curr_link = links_by_endpoint[pair]
        # log.debug("check link %s has current avail_cap %s" % (pair, curr_link.avail_cap))
        if curr_link == origin_link:
            end_op_ids.add(segment_id)
            return is_dependent_to_loop

        if not is_dependent_to_loop:
            loop_ops, is_dependent_to_loop = check_loop_factor(segments_by_seg_path_id, curr_link, segment_id)
        else:
            loop_ops = check_loop_factor(segments_by_seg_path_id, curr_link, segment_id)[0]
        # log.debug("loop_ops: %s" % loop_ops)

        log.debug("loop_ops: %s" % loop_ops)
        if loop_ops:
            for op in loop_ops:
                if not list_checked_segment_ids.has_key(op.seg_path_id):
                    queue.append(op)
                    list_checked_segment_ids[op.seg_path_id] = True

                    # log.debug("stack: %s" % stack)
    return is_dependent_to_loop


def check_loop_factor(segments_by_seg_path_id, link, segment_id):
    return_cap = 0
    for rm_op in link.to_removes:
        if rm_op.seg_path_id != segment_id:
            return_cap += segments_by_seg_path_id[rm_op.seg_path_id].vol
    avail_cap = link.avail_cap + return_cap

    return_ops = []
    required_vol = 0
    is_dependent_to_loop = False
    for add_op in link.to_adds:
        required_vol += segments_by_seg_path_id[add_op.seg_path_id].vol
        if segments_by_seg_path_id[add_op.seg_path_id].vol > avail_cap:
            is_dependent_to_loop = True

    # is_dependent_to_loop = (required_vol > avail_cap)
    log.debug("dependent to loop %s, link %d->%d has at most %s capacity, while requires %s" %
              (is_dependent_to_loop, link.src, link.dst, avail_cap, required_vol))
    return_ops.extend(link.to_adds)
    return return_ops, is_dependent_to_loop


def traverse_op_for_deadlock(stack, segments_by_seg_path_id, links_by_endpoint,
                             segment_id, origin_link, origin_op, end_op_ids, list_checked_segment_ids,
                             total_end_op_vol, is_checking_loop=False):
    log.debug("old segment %s" % segments_by_seg_path_id[segment_id].old_link_seg)
    for pair in segments_by_seg_path_id[segment_id].old_link_seg:
        log.debug("check link %s" % str(pair))
        curr_link = links_by_endpoint[pair]
        log.debug("check link %s has current avail_cap %s" % (pair, curr_link.avail_cap))
        if curr_link == origin_link:
            end_op_ids.add(segment_id)
            total_end_op_vol += segments_by_seg_path_id[segment_id].vol
            if is_checking_loop:
                return True
            if origin_link.avail_cap + origin_link.released_cap - total_end_op_vol < \
                    segments_by_seg_path_id[origin_op.seg_path_id].vol:
                return True
            continue

        loop_ops = check_deadlock_factor(segments_by_seg_path_id, curr_link, segment_id)
        log.debug("loop_ops: %s" % loop_ops)

        if loop_ops:
            for op in loop_ops:
                if not list_checked_segment_ids.has_key(op.seg_path_id):
                    stack.append(op)
                    list_checked_segment_ids[op.seg_path_id] = True
    return False


def check_deadlock_factor(segments_by_seg_path_id, link, segment_id):
    # for rm_op in link.to_removes:
    #     if rm_op.seg_path_id != segment_id:
    #         return_cap += segments_by_seg_path_id[rm_op.seg_path_id].vol
    return_cap = link.released_cap - segments_by_seg_path_id[segment_id].vol
    avail_cap = link.avail_cap + return_cap

    return_op = []
    for add_op in link.to_adds + link.to_adds_loop:
        if segments_by_seg_path_id[add_op.seg_path_id].vol > avail_cap:
            # log.debug("link %d->%d has at most %s capacity" % (link.src, link.dst, avail_cap))
            # log.debug("adding segment %s requires %s capacity"
            #           % (str(add_op.seg_path_id),
            #              segments_by_seg_path_id[add_op.seg_path_id].vol))
            return_op.append(add_op)
    return return_op


def get_total_availability_except(segments_by_seg_path_id, link, segment_id):
    return_cap = link.released_cap - segments_by_seg_path_id[segment_id].vol
    # for rm_op in link.to_removes:
    #     if rm_op.seg_path_id != segment_id:
    #         return_cap += segments_by_seg_path_id[rm_op.seg_path_id].vol
    avail_cap = link.avail_cap + return_cap
    return avail_cap


def get_total_required_vol_except(segments_by_seg_path_id, link, segment_id):
    if segment_id is not None:
        return link.required_cap - segments_by_seg_path_id[segment_id].vol
    else:
        return link.required_cap
