from collections import defaultdict
import itertools

from ez_lib import ez_flow_tool
from domain.network_premitives import *
from domain.message import InstallUpdateMessage
from misc import global_vars, logger, constants


class EzCtrlHandler(object):
    def __init__(self):
        self.log = self.init_logger()
        self.do_segmentation = True

    def __str__(self):
        return "Controller"

    @staticmethod
    def init_logger():
        return logger.getLogger("ControllerHandler", constants.LOG_LEVEL)

    def do_install_update(self, old_flows, new_flows, test_number, skip_deadlock):
        # Update contains a map from the current paths to the target paths for each flow
        segments_by_seg_path_id = defaultdict(LinkSegment)
        links_by_endpoints = defaultdict(Link)
        new_pred_by_sw_and_flow = defaultdict(lambda: defaultdict())
        new_succ_by_sw_and_flow = defaultdict(lambda: defaultdict())
        old_pred_by_sw_and_flow = defaultdict(lambda: defaultdict())
        old_succ_by_sw_and_flow = defaultdict(lambda: defaultdict())

        ez_flow_tool.split_flows(old_flows, new_flows)
        ez_flow_tool.compute_available_link_capacity(links_by_endpoints, old_flows, new_flows)
        for old_flow, new_flow in itertools.izip(old_flows, new_flows):
            self.log.debug("old flow: %s" % old_flow)
            self.log.debug("new flow: %s" % new_flow)
            ez_flow_tool.path_to_ops_by_link(old_flow.flow_id, links_by_endpoints,
                                           segments_by_seg_path_id,
                                           old_flow, new_flow, False, self.do_segmentation)
            for i, j in itertools.izip(old_flow.path[0:len(old_flow.path)-1],
                                       old_flow.path[1:len(old_flow.path)]):
                old_succ_by_sw_and_flow[i][old_flow.flow_id] = (j, False)
                old_pred_by_sw_and_flow[j][old_flow.flow_id] = (i, False)
            for i, j in itertools.izip(new_flow.path[0:len(new_flow.path)-1],
                                       new_flow.path[1:len(new_flow.path)]):
                new_succ_by_sw_and_flow[i][new_flow.flow_id] = (j, False)
                new_pred_by_sw_and_flow[j][new_flow.flow_id] = (i, False)

            old_succ_by_sw_and_flow[old_flow.dst][old_flow.flow_id] = None, None
            old_pred_by_sw_and_flow[old_flow.src][old_flow.flow_id] = None, None
            new_succ_by_sw_and_flow[old_flow.dst][new_flow.flow_id] = None, None
            new_pred_by_sw_and_flow[old_flow.src][new_flow.flow_id] = None, None

        # self.log.info(links_by_endpoints)
        # self.log.info(segments_by_seg_path_id)

        new_msgs = []
        for sw in global_vars.switch_ids:
            msg = InstallUpdateMessage(str(self), sw, links_by_endpoints, segments_by_seg_path_id,
                                       new_pred_by_sw_and_flow[sw],
                                       new_succ_by_sw_and_flow[sw],
                                       old_pred_by_sw_and_flow[sw],
                                       old_succ_by_sw_and_flow[sw], test_number, skip_deadlock)
            new_msgs.append(msg)

        return new_msgs
