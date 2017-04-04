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
import ryu.utils as utils
from collections import deque
import eventlet
import struct
from eventlet.green import socket

sys.path.append(os.path.realpath("../simulator"))

from ez_lib import ez_switch_handler
from ez_lib.ez_topo import Ez_Topo
from misc import constants, global_vars
from domain.message import AggregatedMessage, NotificationMessage
import requests
from time import time
from datetime import datetime

from topo.topo_factory import TopoFactory


class SimpleSwitch13(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    LOGGER_NAME = "local_ctrl"
    # _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(SimpleSwitch13, self).__init__(*args, **kwargs)

        # wsgi = kwargs['wsgi']
        # wsgi.register(RestStatController, {'ezsegway_lctrl': self})
        # mapper = wsgi.mapper

        # uri = '/ezsegway/update/{switch_id}'
        # mapper.connect('ezsegway', uri,
        #                controller=RestStatController, action='post_update',
        #                conditions=dict(method=['POST']))

        self.notification_queue = deque([])

        self.switch_id = int(os.environ.get("EZSWITCH_ID", 0))
        self.topo_input = os.environ.get("TOPO_INPUT", 1)
        # self.topo_type = os.environ.get("TOPO_TYPE", 2)
        # self.generating_method = os.environ.get("GENERATING_METHOD", 3)
        # self.number_of_flows = int(os.environ.get("NUMBER_OF_FLOW", 4))
        # self.failure_rate = float(os.environ.get("FAILURE_RATE", 5))
        # self.repeat_time = int(os.environ.get("REPEAT_TIMES", 6))
        self.datapath = None
        self.eth_to_port = {}
        self.logger.info("switch_id=%d" % self.switch_id)
        self.logger.info("topo_input=%s" % self.topo_input)
        self.logger.level = constants.LOG_LEVEL
        self.current_starting_time = None
        self.current_starting_date_time = None
        self.start_receiving_update_time = None
        self.finishing_update_local = None
        self.finishing_update_local_date_time = None
        self.finishing_computing_time = None
        self.computation_time_in_ctrl = None
        self.test_number = 0
        self.sw_to_ctrl_delay = 0


        t = TopoFactory.create_topo(self.topo_input)
        self.topo = t.extract_topo()

        # initialize the handler based on switch id
        neighbors = map(lambda x: x - 1, self.topo[self.switch_id + 1].keys())
        self.handler = ez_switch_handler.EzSwitchHandler(self.switch_id, None, neighbors, self.callback_func)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        if self.datapath is None:
            self.datapath = datapath
        else:
            raise Exception('Only one switch can connect!')
        self.logger.info(datapath)
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

        self.logger.info("datapath id %s", datapath.id)

        match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP, ipv4_dst=ipAdd(datapath.id))
        actions = [parser.OFPActionOutput(1)]
        self.add_flow(datapath, 1, match, actions)
        hub.spawn(self.run_server)

        self.create_topology_info()

    def create_topology_info(self):
        data_directory = "../%s/%s" % ("data", self.topo_input)
        self.create_topology_from_adjacency_matrix(data_directory)

    def create_topology_from_adjacency_matrix(self, data_directory):
        topo_file = data_directory + "/%s" % constants.ADJACENCY_FILE
        ez_topo = Ez_Topo()
        # topo = ez_topo.create_rocketfuel_topology(data_directory)
        topo = ez_topo.create_latency_topology_from_adjacency_matrix(data_directory)
        global_vars.switch_ids = topo.graph.nodes()

    def send_finish_msg(self):
        self.finishing_update_local = time() * 1000
        self.finishing_update_local_exclude_transfer_time = self.finishing_update_local \
                                                            - self.finish_receiving_update_time
        max_no_splitting_segments = self.handler.scheduler.trace.max_splitting_segments
        self.handler.scheduler.trace.convert_to_time_from_starting(self.start_receiving_update_time, self.computation_time_in_ctrl)

        # self.finishing_update_local_date_time = datetime.now()
        # self.logger.info("time to finish update: %s"
        #                  % str(self.finishing_update_local - self.current_starting_time))
        self.logger.info("time to finish update: %s"
                         % str(self.finishing_update_local - self.start_receiving_update_time))

        # self.receiving_update_time = None
        feedback_msg = pickle.dumps({'switch_id': self.switch_id,
                                     'finish_receiving_time': self.finish_receiving_update_time,
                                     'start_receiving_time': self.start_receiving_update_time,
                                     'finishing_time': self.finishing_update_local,
                                     'finishing_time_no_transfer':
                                         self.finishing_update_local_exclude_transfer_time,
                                     'finishing_computation_time': self.finishing_computing_time,
                                     'max_splitting_segments': max_no_splitting_segments,
                                     'having_deadlock': self.handler.scheduler.scheduling_mode,
                                     'max_working_rules': self.handler.scheduler.trace.max_no_of_working_rules,
                                     'times_using_new_path_strs':
                                         self.handler.scheduler.trace.times_using_new_path_to_string(),
                                     'rule_overheads': self.handler.scheduler.trace.list_to_string(
                                         self.handler.scheduler.trace.list_overhead_infos),
                                     'msgs': self.handler.scheduler.trace.list_msgs_with_sending_time,
                                     'number_msgs' : self.handler.scheduler.trace.no_of_received_messages,
                                     'times_using_new_path':
                                         self.handler.scheduler.trace.time_using_new_path_by_seg_path_id,
                                     'time_next_sw': self.handler.scheduler.trace.time_new_next_sw_by_seg_path_id})
                                     # 'starting_date_time': self.current_starting_date_time,
                                     # 'finishing_date_time': self.finishing_update_local_date_time})
        requests.post("http://127.0.0.10:8800", data=feedback_msg)


    def process_install_msg(self, msg):
        # all switch id coming from the handler zero based
        self.finish_receiving_update_time = time() * 1000
        self.computation_time_in_ctrl = msg.computation_time_in_ctrl
        update_infos, finished, finishing_computing_time = self.handler.do_install_update(msg)

        self.finishing_computing_time = finishing_computing_time - self.finish_receiving_update_time
        self.logger.debug("update_info: %s in switch having id: %d. Finished = %s"
                         % (update_infos, self.switch_id, finished))
        self.install_updates(self.datapath, update_infos)

        if finished == 1:
            # hub.spawn_after(global_vars.sw_to_ctrl_delays[self.switch_id]/1000, self.send_finish_msg)
            hub.spawn_after(self.sw_to_ctrl_delay / 1000, self.send_finish_msg)

    def install_updates(self, datapath, update_infos):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        current_time = time() * 1000
        # if self.receiving_update_time is not None:
        #     self.logger.info("processing updates at %s" % str(current_time - self.receiving_update_time))

        for update in update_infos:
            # src+1, dst+1 -> IPs
            src_ip = ipAdd(update.init_sw + 1)
            dst_ip = ipAdd(update.end_sw + 1)

            self.logger.debug("process update %s" % update)

            if update.update_next_sw.type == constants.ADD_NEXT:

                out_port = self.topo[datapath.id][update.update_next_sw.next_sw + 1]

                match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP, ipv4_src=src_ip, ipv4_dst=dst_ip)
                actions = [parser.OFPActionOutput(out_port)]

                self.add_flow(datapath, 2, match, actions)

            elif update.update_next_sw.type == constants.UPDATE_NEXT:
                out_port = self.topo[datapath.id][update.update_next_sw.next_sw + 1]

                match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP, ipv4_src=src_ip, ipv4_dst=dst_ip)
                actions = [parser.OFPActionOutput(out_port)]

                self.update_flow(datapath, 2, match, actions)

            elif update.update_next_sw.type == constants.REMOVE_NEXT:
                match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP, ipv4_src=src_ip, ipv4_dst=dst_ip)

                self.remove_flow(datapath, 2, match)

            elif update.update_next_sw.type == constants.NO_UPDATE_NEXT:
                pass  # Do nothing

            else:
                raise Exception("what type?")

            if len(update.msgs) > 0:
                self.notification_queue.extend(update.msgs)

        # if self.receiving_update_time is not None:
        #     current_time = time() * 1000
        #     self.logger.info("sending barrier at %s" % str(current_time - self.receiving_update_time))
        datapath.send_barrier()

    def aggregate_msgs(self, msgs):
        f_msg = msgs[0]
        agg_msg = AggregatedMessage(f_msg.src_id, f_msg.dst_id, f_msg.msg_type, [], f_msg.update_id)
        for msg in msgs:
            agg_msg.seg_path_ids.append(msg.seg_path_id)
        return agg_msg

    def split_msgs(self, agg_msg):
        msgs = []
        for key in agg_msg.seg_path_ids.keys():
            for seg_path_id in agg_msg.seg_path_ids[key]:
                msgs.append(NotificationMessage(agg_msg.src_id, agg_msg.dst_id,
                                                key, seg_path_id, agg_msg.update_id,
                                                agg_msg.sending_time, agg_msg.receiving_time))
        return msgs

    @set_ev_cls(ofp_event.EventOFPBarrierReply, MAIN_DISPATCHER)
    def _handle_barrier(self, ev):
        to_switches = {}
        # if self.receiving_update_time is not None:
        #     current_time = time() * 1000
        #     self.logger.info("receiving barrier at %s" % str(current_time - self.receiving_update_time))
        # sent_this_time = list(self.notification_queue)
        while len(self.notification_queue) > 0:
            msg = self.notification_queue.popleft()
            if not to_switches.has_key(msg.dst_id):
                to_switches[msg.dst_id] = [AggregatedMessage(msg.src_id, msg.dst_id, msg.msg_type, [],
                                                            msg.update_id)]
            l_len = len(to_switches[msg.dst_id])
            len_msg = 0
            for l_msg in to_switches[msg.dst_id][l_len - 1].seg_path_ids.values():
                len_msg += len(l_msg)
            if len_msg > 30:
                to_switches[msg.dst_id].append(AggregatedMessage(msg.src_id, msg.dst_id, msg.msg_type, [],
                                                        msg.update_id))

            if not to_switches[msg.dst_id][l_len-1].seg_path_ids.has_key(msg.msg_type):
                to_switches[msg.dst_id][l_len-1].seg_path_ids[msg.msg_type] = [msg.seg_path_id]
            else:
                to_switches[msg.dst_id][l_len-1].seg_path_ids[msg.msg_type].append(msg.seg_path_id)
            # if not to_switches.has_key((msg.dst_id, msg.msg_type)):
            #     to_switches[(msg.dst_id, msg.msg_type)] = [msg]
            # else:
            #     to_switches[(msg.dst_id, msg.msg_type)].append(msg)

        # agg_msgs = {}
        for l_msg in to_switches.values():
            for msg in l_msg:
                # agg_msg = self.aggregate_msgs(to_switches[(dst, msg_type)])
                # self.logger.debug("message: %s" % str(agg_msg))
                self.send_msg(self.datapath, msg)
        # if sent_this_time:
        #     self.handler.scheduler.trace.add_trace_for_msgs(time() * 1000, sent_this_time)

    def send_msg(self, datapath, msg):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        out_port = self.topo[datapath.id][msg.dst_id + 1]
        actions = [parser.OFPActionOutput(out_port)]

        src_mac = self.macStr(msg.src_id + 1)
        dst_mac = self.macStr(msg.dst_id + 1)

        src_ip = ipStr(msg.src_id + 1)
        dst_ip = ipStr(msg.dst_id + 1)
        self.logger.debug("sending message from %s to %s" % (src_ip, dst_ip))
        udp_port = 6620

        e = ethernet.ethernet(dst=dst_mac,
                              src=src_mac,
                              ethertype=ether.ETH_TYPE_IP)
        sending_time = time() * 1000
        msg.sending_time = sending_time
        pmsg = pickle.dumps(msg)
        i = ipv4.ipv4(src=src_ip,
                      dst=dst_ip,
                      proto=IPPROTO_UDP,
                      total_length=ipv4.ipv4._MIN_LEN + udp.udp._MIN_LEN + len(pmsg))
        u = udp.udp(dst_port=udp_port,
                    src_port=udp_port,
                    total_length=udp.udp._MIN_LEN + len(pmsg))
        self.logger.debug("message length: %d" % len(pmsg))

        pkt = packet.Packet()
        pkt.add_protocol(e)
        pkt.add_protocol(i)
        pkt.add_protocol(u)
        pkt.serialize()
        pkt.data += bytearray(pmsg)
        # FIXME: pmsg could be more than maximum payload ~1450 bytes

        out = parser.OFPPacketOut(datapath=datapath, in_port=ofproto.OFPP_CONTROLLER,
                                  buffer_id=ofproto.OFP_NO_BUFFER, actions=actions, data=pkt.data)
        if self.start_receiving_update_time is not None:
            current_time = time() * 1000
            # self.logger.info("sending msg to %d at %s" %
            #                  (msg.dst_id, str(current_time - self.receiving_update_time)))
        datapath.send_msg(out)

    def macStr(self, mac):
        a = '%012x' % mac
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

    def process_ezsegway_notification_msg(self, pkt, datapath, receiving_time=time()):
        agg_msg = pickle.loads(pkt.protocols[-1])
        receiving_time *= 1000
        agg_msg.receiving_time = receiving_time
        self.logger.info("msg %s is transfer in: %s ms" % (str(agg_msg),
                                                         agg_msg.receiving_time - agg_msg.sending_time))
        # if self.receiving_update_time is not None:
        #     elapsed_time = agg_msg.receiving_time - self.receiving_update_time
        #     self.logger.info("msg %s is received at %s ms since starting" %
        #                      (str(agg_msg), str(elapsed_time)))
        self.logger.debug("receive messages: %s", agg_msg)
        notification_msgs = self.split_msgs(agg_msg)
        self.handler.scheduler.trace.add_trace_for_msgs(notification_msgs)
        update_info_s = []

        finished = 0
        for notification_msg in notification_msgs:
            if finished != 1:
                update_infos, finished = self.handler.do_handle_notification(notification_msg)
            else:
                update_infos, no_care = self.handler.do_handle_notification(notification_msg)
            self.logger.debug("finished: %s for msg: %s" % (finished, notification_msg))
            update_info_s.extend(update_infos)
        self.logger.debug(update_info_s)
        self.install_updates(datapath, update_info_s)
        if finished == 1:
            # hub.spawn_after(global_vars.sw_to_ctrl_delays[self.switch_id]/1000, self.send_finish_msg)
            hub.spawn_after(self.sw_to_ctrl_delay / 1000, self.send_finish_msg)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # If you hit this you might want to increase
        # the "miss_send_length" of your switch
        receiving_time = time()
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        in_port = msg.match['in_port']
        dpid = datapath.id

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        dst = eth.dst
        src = eth.src

        udp_pkt = pkt.get_protocol(udp.udp)
        if udp_pkt and udp_pkt.dst_port == 6620:
            self.process_ezsegway_notification_msg(pkt, datapath, receiving_time)

        # learn a mac address to avoid FLOOD next time.
        self.eth_to_port.setdefault(dpid, {})
        self.eth_to_port[dpid][src] = in_port

        if dst in self.eth_to_port[dpid]:
            out_port = self.eth_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [datapath.ofproto_parser.OFPActionOutput(out_port)]

        buffer_id = ofproto.OFP_NO_BUFFER if msg.buffer_id == None else msg.buffer_id
        out = datapath.ofproto_parser.OFPPacketOut(
            datapath=datapath, buffer_id=buffer_id, in_port=in_port,
            actions=actions)
        datapath.send_msg(out)

    @set_ev_cls(ofp_event.EventOFPErrorMsg,
                [HANDSHAKE_DISPATCHER, CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def error_msg_handler(self, ev):
        msg = ev.msg

        self.logger.debug('OFPErrorMsg received: type=0x%02x code=0x%02x '
                          'message=%s',
                          msg.type, msg.code, utils.hex_array(msg.data))

    def connection_handler(self, fd):
        while True:
            data = fd.recv(24)
            if len(data) == 0:
                fd.close()
                return
            self.start_receiving_update_time = time() * 1000
            msg_len, sending_time, self.sw_to_ctrl_delay = struct.unpack('Ldd', data)
            self.logger.debug("length: %d" % msg_len)
            pickle_msg = recv_size(fd, msg_len)
            self.logger.debug("recv length: %d" % len(pickle_msg))
            msg = pickle.loads(pickle_msg)

            # self.logger.info("Receive message from global ctrl %s ms after sending (at %s)"
            #                  % (str(self.current_time_to_transfer_install_msg), time_in_date))
            self.logger.debug(msg)
            hub.spawn_after(self.sw_to_ctrl_delay / 1000, self.process_install_msg, msg)
            # self.process_install_msg(msg)

    def run_server(self):
        server = eventlet.listen(('127.0.0.1', 6800 + self.switch_id))
        while True:
            fd, addr = server.accept()
            self.logger.debug("receive a connection")
            self.connection_handler(fd)

    def callback_func(self, update_infos, finished):
        self.install_updates(self.datapath, update_infos)
        # self.logger.info("Invoked callback: %s" % update_infos)
        if finished == 1:
            #hub.spawn_after(global_vars.sw_to_ctrl_delays[self.switch_id]/1000, self.send_finish_msg)
            hub.spawn_after(self.sw_to_ctrl_delay / 1000, self.send_finish_msg)


def recv_size(fd, size):
    total_len=0
    total_data=[]
    while total_len < size:
        sock_data = fd.recv(size - total_len)
        total_data.append(sock_data)
        total_len = sum([len(i) for i in total_data ])
    return ''.join(total_data)
