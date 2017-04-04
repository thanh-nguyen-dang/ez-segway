# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ryu import cfg
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, HANDSHAKE_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, ipv4, udp
from ryu.lib.packet import ether_types
from ryu.ofproto import ether
from ryu.lib.packet.in_proto import IPPROTO_UDP
from mininet.util import ipAdd, ipStr

from ryu.app.wsgi import ControllerBase, WSGIApplication, route
from ryu.lib import dpid as dpid_lib
from webob import Response

import logging
import cPickle as pickle
import sys
import os
import signal
import ryu.utils as utils
import threading
import jsonpickle

sys.path.append(os.path.realpath("../simulator"))

from ez_lib.cen_ctrl_handler import CenCtrlHandler
from ez_lib.ez_topo import Ez_Topo
from flow_gen.flow_change_generator import FlowChangeGenerator
from domain.message import NotificationMessage
from misc import constants, utils, global_vars, logger
from time import time
from random import Random

from topo.topo_factory import TopoFactory
logger.init('../logs/cen_result.log', level=logging.INFO)
log = logger.getLogger('cen_result', logging.INFO)

class SimpleSwitch13(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SimpleSwitch13, self).__init__(*args, **kwargs)

        self.eth_to_port = {}
        self.logger.info("central_ctrl")
        self.logger.level = constants.LOG_LEVEL

        self.datapaths = {}

        self.topo_input = os.environ.get("TOPO_INPUT", 0)
        t = TopoFactory.create_topo(self.topo_input)
        self.topo = t.extract_topo()

        switches = [x-1 for x in self.topo.keys()]
        self.handler = CenCtrlHandler(sorted(switches), self.logger)
        self.rng = Random()
        # self.notification_queues = {x: deque([]) for x in self.topo.keys()}

        # self.no_of_pending_msgs = {}
        # self.current_notification_time = {x: -1 for x in self.topo.keys()}
        # self.current_processing_time = {x: -1 for x in self.topo.keys()}

        self.thread = None

        self.current_controller_sw = 0
        self.current_update = -1
        self.current_start_time = None
        self.current_sending_time = None
        self.current_finish_sending_time = None
        self.current_dependency_graph_cal = None
        self.test_number = 0
        self.encounter_deadlock = False
        self.create_topology("../data/%s/" % self.topo_input)

    def delete(self):
        if self.thread is not None:
            hub.kill(self.thread)
            self.thread.wait()


    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        self.datapaths[datapath.id] = datapath

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser


        # install table-miss flow entry
        #
        # We specify NO BUFFER to max_len of the output action due to
        # OVS bug. At this moment, if we specify a lesser number, e.g.,
        # 128, OVS will send Packet-In with invalid buffer_id and
        # truncated packet data. In that case, we cannot output packets
        # correctly.  The bug has been fixed in OVS v2.1.0.
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

        self.logger.debug("datapath id %s", datapath.id)

        match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP, ipv4_dst=ipAdd(datapath.id))
        actions = [parser.OFPActionOutput(1)]
        self.add_flow(datapath, 1, match, actions)

        hub.patch(socket=True, thread=True, os=True, select=True)
        if len(self.datapaths) == len(self.topo):
            # All switches have connected
            # Can call to install new path from here
            hub.spawn_after(10, self._cyclic_update)

    def _cyclic_update(self):
        update = self.read_flows(self.test_number)
        if update.old_flows == [] and update.new_flows == []:
            # self.logger.info("Having deadlock")
            return
        self.call_to_install_update(update.old_flows, update.new_flows)
        self.test_number += 1

    def process_update_info(self, datapath, sw, update_next, init_sw, end_sw):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # src+1, dst+1 -> IPs
        src_ip = ipAdd(init_sw + 1)
        dst_ip = ipAdd(end_sw + 1)

        self.logger.debug("process update %s of flow between %s, %s" % (update_next, src_ip, dst_ip))

        if update_next.type == constants.ADD_NEXT or update_next.type == constants.UPDATE_NEXT:

            out_port = self.topo[datapath.id][update_next.next_sw + 1]

            match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP, ipv4_src=src_ip, ipv4_dst=dst_ip)
            actions = [parser.OFPActionOutput(out_port)]

            self.add_flow(datapath, 2, match, actions)
            msg = NotificationMessage(sw, global_vars.ctrl,
                                      constants.UPDATED_MSG,
                                      update_next.seg_path_id,
                                      self.current_update,
                                      time() * 1000)
            self.handler.scheduler.enque_msg_to_notification_queue(sw, msg)
            # notification_queue.append(msg)
            #
            # self.no_of_pending_msgs[(datapath.id, self.current_notification_time[datapath.id])] += 1

        elif update_next.type == constants.REMOVE_NEXT:
            match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP, ipv4_src=src_ip, ipv4_dst=dst_ip)

            self.remove_flow(datapath, 2, match)
            msg = NotificationMessage(sw, global_vars.ctrl,
                                      constants.REMOVED_MSG,
                                      update_next.seg_path_id,
                                      self.current_update,
                                      time() * 1000)
            # notification_queue.append(msg)
            # self.no_of_pending_msgs[(datapath.id, self.current_notification_time[datapath.id])] += 1
            self.handler.scheduler.enque_msg_to_notification_queue(sw, msg)

        elif update_next.type == constants.NO_UPDATE_NEXT:
            pass  # Do nothing

        else:
            raise Exception("what type?")


    @set_ev_cls(ofp_event.EventOFPBarrierReply, MAIN_DISPATCHER)
    def _handle_barrier(self, ev):
        dpid = ev.msg.datapath.id
        # self.logger.info("barrier from switch %d, invoke at time: %s" % (dpid, (time() - self.current_start_time) * 1000))

        delay = global_vars.sw_to_ctrl_delays[dpid-1] * self.rng.uniform(0.95, 1.1)
        latency = 2 * (delay/1000)
        self.logger.debug("latency: %s ms" % str(latency * 1000))
        # hub.spawn_after(latency, self._progress_update, dpid)
        hub.spawn_after(latency, self.handler.do_handle_barrier_from_sw, dpid-1,
                        self.call_process_update_info, self.call_to_send_barrier,
                        self.do_when_finish)

    def do_when_finish(self, encounter_deadlock):
        finished_time = time() * 1000
        finish_time_from_start = finished_time - self.current_start_time
        finish_time_from_last_sending = finished_time - self.current_finish_sending_time
        total_sending_time = self.current_finish_sending_time - self.current_sending_time
        update_only_time = (finish_time_from_start - self.current_dependency_graph_cal)
        max_delay = max(global_vars.sw_to_ctrl_delays)
        self.handler.scheduler.trace.convert_to_time_from_starting(self.current_finish_sending_time,
                                                                   self.current_sending_time - self.current_start_time
                                                                   + max_delay
                                                                   )
        log.info("test-%d: %f\t%d\t%f\t%f\t%f\t%f\t%d\t%s" %
                 (self.test_number - 1, finish_time_from_start, 0,
                  self.current_dependency_graph_cal, update_only_time,
                  finish_time_from_last_sending, total_sending_time,
                  self.handler.message_count * 2, encounter_deadlock))
        log.info("test-%d-new_path: %s" % ((self.test_number - 1),
                                           self.handler.scheduler.trace.times_using_new_path_to_string()))
        # log.info("calculating time: %d ms" % self.current_dependency_graph_cal)
        # log.info("finished after %s ms from sending" % (finish_time_from_sending * 1000))
        if self.test_number < 1000:
            hub.spawn_after(1, self._cyclic_update)
        else:
            os.kill(os.getpid(), signal.SIGTERM)
            return
            # if self.current_controller_sw < len(global_vars.switch_ids) - 1:
            #     self.current_controller_sw += 1
            #     self.test_number = 0
            #     self.ez_topo.deploy_controller(self.current_controller_sw, global_vars.sw_to_ctrl_delays)
            #     hub.spawn_after(1, self._cyclic_update)


    def macStr(self, mac):
        a = '%012x'% mac
        b = ':'.join(s.encode('hex') for s in a.decode('hex'))
        return b

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    def update_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, command=ofproto.OFPFC_MODIFY_STRICT, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, command=ofproto.OFPFC_MODIFY_STRICT, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    def remove_flow(self, datapath, priority, match):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        mod = parser.OFPFlowMod(datapath=datapath, command=ofproto.OFPFC_DELETE,
                                out_port=ofproto.OFPP_ANY, out_group=ofproto.OFPG_ANY,
                                match=match, priority=priority)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # If you hit this you might want to increase
        # the "miss_send_length" of your switch
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)
        # msg = ev.msg
        # datapath = msg.datapath
        ofproto = ev.msg.datapath.ofproto
        in_port = ev.msg.match['in_port']
        dpid = ev.msg.datapath.id

        pkt = packet.Packet(ev.msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        dst = eth.dst
        src = eth.src
        # self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)

        # learn a mac address to avoid FLOOD next time.
        self.eth_to_port.setdefault(dpid, {})
        self.eth_to_port[dpid][src] = in_port

        if dst in self.eth_to_port[dpid]:
            out_port = self.eth_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [ev.msg.datapath.ofproto_parser.OFPActionOutput(out_port)]

        buffer_id = ofproto.OFP_NO_BUFFER if ev.msg.buffer_id == None else ev.msg.buffer_id
        out = ev.msg.datapath.ofproto_parser.OFPPacketOut(
                datapath=ev.msg.datapath, buffer_id=buffer_id, in_port=in_port,
                actions=actions)
        ev.msg.datapath.send_msg(out)


    @set_ev_cls(ofp_event.EventOFPErrorMsg,
            [HANDSHAKE_DISPATCHER, CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def error_msg_handler(self, ev):
        msg = ev.msg

        self.logger.debug('OFPErrorMsg received: type=0x%02x code=0x%02x '
                          'message=%s',
                          msg.type, msg.code, utils.hex_array(msg.data))

    def call_to_install_update(self, old_flows, new_flows):
        # self.current_notification_time = {x: -1 for x in self.topo.keys()}
        # self.current_processing_time = {x: -1 for x in self.topo.keys()}
        # self.no_of_pending_msgs.clear()
        # self.no_of_pending_msgs = {}#(x, 0): 0 for x in self.topo.keys()}

        # self.logger.info('Starting installing update')
        self.current_start_time = time() * 1000
        update_infos, dependency_time = self.handler.do_install_update(old_flows, new_flows)
        self.current_dependency_graph_cal = dependency_time * 1000 - self.current_start_time
        self.current_sending_time = time() * 1000 
        self.handler.handle_new_update_infos(update_infos, self.call_process_update_info,
                                             self.call_to_send_barrier)
        self.current_finish_sending_time = time() * 1000
        #self.logger.info("finish sending barrier at %s" % (self.current_finish_sending_time - self.current_start_time))

    # def send_notification_msgs(self, update_infos):
        # Send to data plan in different port according to specified switch in update_info
        # increased = set()
        # barrier_datapaths = set([])
        # for key in update_infos.keys():
        #     update_info = update_infos[key]
        #     self.logger.debug("data paths: %s" % str(self.datapaths))
        #     # self.logger.info("Process update info %s at %d ms from starting" % (update_info, (time() - self.current_start_time)*1000))
        #     assert update_info, CenUpdateInfo
        #     for sw in update_infos[key].update_nexts.keys():
        #         self.logger.debug("switch: %s" % sw)
        #         self.logger.debug("current notification time %s" % self.current_notification_time)
        #         self.logger.debug("increased %s" % increased)
        #         if (sw + 1) not in increased:
        #             self.current_notification_time[sw+1] += 1
        #             increased.add(sw + 1)
        #             self.no_of_pending_msgs[(sw + 1, self.current_notification_time[sw + 1])] = 0
        #         update_next = update_info.update_nexts[sw]
        #         assert update_next, UpdateNext
        #         self.process_update_info(self.datapaths[sw + 1], sw,
        #                                  update_next, update_info.init_sw, update_info.end_sw,
        #                                  self.notification_queues[sw + 1])
        #         self.logger.debug("add message in processing update_info: %s" % update_info)
        #         self.logger.debug("pending messages: %s" % str(self.no_of_pending_msgs))
        #         barrier_datapaths.add(sw + 1) #self.datapaths[sw + 1])
        # related_sws = self.handler.update_message_queues(update_infos,
        #                                                   self.call_process_update_info)


    def call_to_send_barrier(self, related_sws):
        for sw in related_sws:
            hub.spawn(self.datapaths[sw + 1].send_barrier)

    def call_process_update_info(self, sw, update_info):
        update_next = update_info.update_nexts[sw]
        self.process_update_info(self.datapaths[sw + 1], sw,
                                 update_next, update_info.init_sw, update_info.end_sw)

    def read_flows(self, test_number):
        flow_gen = FlowChangeGenerator()
        directory = "../data/%s/random/1000/" % self.topo_input
        filename = directory + "flows_%d.intra" % test_number
        self.logger.debug(filename)
        return flow_gen.read_flows(filename)


    def create_topology(self, data_directory):
        ez_topo_creator = Ez_Topo()
        self.ez_topo = ez_topo_creator.create_latency_topology_from_adjacency_matrix(data_directory, -1)
        # self.ez_topo = ez_topo_creator.create_rocketfuel_topology(data_directory)
        global_vars.switch_ids = self.ez_topo.graph.nodes()
        # self.logger.debug("topo: %s" % str(topo.graph))
        # self.logger.info("delay from ctrl to sw: %s" % global_vars.sw_to_ctrl_delays)
        # self.logger.info("delay from sw to sw: %s" % global_vars.sw_to_sw_delays)
