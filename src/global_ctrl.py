#!/usr/bin/python

import argparse
import requests
import cPickle as pickle

import sys
import os

sys.path.append(os.path.realpath("../simulator"))
from flow_gen.flow_change_generator import FlowChangeGenerator
from misc import logger, constants, utils, global_vars
from ez_lib import ez_ctrl_handler as handler
from ez_lib.ez_topo import Ez_Topo
from domain.message import NotificationMessage
import eventlet
import struct
from collections import deque, defaultdict
from eventlet import wsgi
from eventlet.green import socket
from time import time
from datetime import datetime
from collections import OrderedDict
from domain.events import MessageSendingEvent, FlowChangeEvent, FlowTableChangeEvent
import json

class EzGlobalController(object):
    def __init__(self, log_, repeat_time_, skip_deadlock):
        self.handler = handler.EzCtrlHandler()
        self.dpid = 0
        self.log = log_
        self.test_number = 0
        self.repeat_time = repeat_time_
        self.sending_msgs_queue = defaultdict(deque)
        self.current_start_time = None
        self.current_sending_time = None
        self.last_sending_time = None
        self.finishing_times = {}
        self.finishing_computation_times = {}
        self.max_splitting_segments = {}
        self.max_no_of_working_rules = {}
        self.times_using_new_path_by_switch_strs = {}
        self.times_using_new_path_by_switch = {}
        self.times_change_next_sw_by_switch = {}
        self.overhead_infos = {}
        self.no_of_received_msgs = 0
        self.sockets = {}
        self.having_splittable_deadlock = False
        self.having_unsplittable_deadlock = False
        self.receiving_backs = {}
        self.msgs_sent_by_sws = {}
        self.current_controller_sw = 0
        self.times_sending_to_switches = {}
        self.times_finishing_receiving_install_msg = {}
        self.times_start_receiving_install_msg = {}
        self.current_computation_time = 0
        if skip_deadlock == 1:
            self.skip_deadlock = True
        else:
            self.skip_deadlock = False
        self.deadlock_count = 0
        self.deadlock_unsplittable_count = 0

    def install_update(self, old_flows, new_flows):
        self.no_of_received_msgs = 0
        self.current_start_time = time() * 1000
        new_msgs = self.handler.do_install_update(old_flows, new_flows, self.test_number, self.skip_deadlock)
        # self.log.info(new_msgs)
        self.current_sending_time = time() * 1000
        self.current_computation_time = self.current_sending_time - self.current_start_time
        self.log.debug("delay from ctrl to sw: %s" % global_vars.sw_to_ctrl_delays)

        pickled_msgs = []
        for new_msg in new_msgs:
            new_msg.computation_time_in_ctrl = self.current_computation_time
            str_message = pickle.dumps(new_msg, pickle.HIGHEST_PROTOCOL)
            pickled_msgs.append(str_message)

        c = 0
        for new_msg in new_msgs:
            # self.send_to_switch(new_msg, pickled_msgs[c])
            # latency = global_vars.sw_to_ctrl_delays[new_msg.dst_id]/1000
            eventlet.spawn_after(0, self.send_to_switch, new_msg, pickled_msgs[c])
            c += 1

    def send_to_switch(self, msg, str_message):
        self.log.debug("send to ctrl: %s" % str(msg.dst_id))
        global_vars.sent_to_sws[msg.dst_id] = True

        # requests.post("http://127.0.0.1:%d/ezsegway/update/%d" % (8733 + msg.dst_id, msg.dst_id), data=str_message)
        msg_len = len(str_message)
        self.log.debug(msg_len)
        # self.log.debug(len(struct.pack('Lf', msg_len, sending_time)))

        sending_time = time() * 1000
        time_in_date = datetime.now()
        # str_message = pickle.dumps(msg)
        # self.log.info("sending to sw %d at %s ms from starting (or at %s)" %
        #               (msg.dst_id, (sending_time - self.current_start_time), time_in_date))
        self.times_sending_to_switches[msg.dst_id] = sending_time

        latency = global_vars.sw_to_ctrl_delays[msg.dst_id]
        data = struct.pack('Ldd', msg_len, sending_time, latency) + str_message
        self.sockets[msg.dst_id].sendall(data)

        if self.last_sending_time == None or self.last_sending_time < sending_time:
            self.last_sending_time = sending_time

    def on_timer(self):
        if self.test_number >= self.repeat_time:
            # self.test_number = self.test_number % self.repeat_time
            self.log.info("deadlock count: %d" % self.deadlock_count)
            self.log.info("unsplittable deadlock count: %d" % self.deadlock_unsplittable_count)
            sys.exit(0)
            return
            # When running on real switch use this:
            #if self.current_controller_sw < len(global_vars.switch_ids) - 1:
            #    self.current_controller_sw += 1
            #    self.test_number = 0
            #else:
            #    return
        flow_gen = FlowChangeGenerator()
        # self.log.info("test number: %d" % self.test_number)
        self.log.debug(self.repeat_time)
        filename = global_vars.flow_folder + "/flows_%s.intra"\
                                             % str(self.test_number % self.repeat_time)
        update = flow_gen.read_flows(filename)
        # self.convert_flows(filename, old_flows, new_flows)

        if update.old_flows == [] and update.new_flows == []:
            self.log.info("has deadlock during transition")
            return
        # self.log.info("old_flows: %s" % old_flows)
        # self.log.info("new_flows: %s" % new_flows)
        log.info("skip deadlock: %s" % self.skip_deadlock)
        self.having_splittable_deadlock = False
        self.having_unsplittable_deadlock = False
        self.msgs_sent_by_sws.clear()
        self.install_update(update.old_flows, update.new_flows)
        self.test_number += 1

    def create_topology_from_adjacency_matrix(self, data_directory):
        topo_file = data_directory + "/%s" % constants.ADJACENCY_FILE
        ez_topo = Ez_Topo()
        # topo = ez_topo.create_rocketfuel_topology(data_directory) #, self.current_controller_sw)
        self.log.info("read from {0}".format(data_directory))
        topo = ez_topo.create_latency_topology_from_adjacency_matrix(data_directory, -1)
        global_vars.switch_ids = topo.graph.nodes()
        self.sending_msgs_queue = {x: deque([]) for x in global_vars.switch_ids}
        for sw_id in global_vars.switch_ids:
            c = socket.socket()
            host = socket.gethostbyname('127.0.0.1')
            c.connect((host, 6800 + sw_id))
            self.sockets[sw_id] = c

    def run_server(self):
        wsgi.server(eventlet.listen(('127.0.0.10', 8800)), self.wsgi_app, max_size=50)

    def run_experiment(self, args):
        data_directory = "../%s/%s" % (args.data_folder, args.topology)
        self.create_topology_from_adjacency_matrix(data_directory)
        eventlet.monkey_patch(socket=True, thread=True)
        global_vars.flow_folder = utils.get_flow_folder(data_directory, args.topology_type, args.generating_method, \
                                            args.number_of_flows, args.failure_rate)
        # self.log.info("flow folder: %s" % global_vars.flow_folder)
        self.on_timer()
        self.run_server()

    def wsgi_app(self, env, start_response):
        # print "Get finished msg"
        input = env['wsgi.input']
        data_feedback = pickle.loads(input.read())
        # self.log.info("Receive message %s via Restful WebServices from switch: %s" %
        #               (data_feedback, data_feedback['switch_id']))
        self.process_feedback(data_feedback)
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return []

    def check_having_splittable_deadlock(self, having_deadlock):
        if having_deadlock & constants.CONGESTION_MODE == constants.CONGESTION_MODE \
                or having_deadlock & constants.SPLITTING_MODE == constants.SPLITTING_MODE:
            return True
        return False

    def check_having_unsplittable_deadlock(self, having_deadlock):
        return having_deadlock & (constants.CONGESTION_MODE | constants.SPLITTING_MODE) == \
               (constants.CONGESTION_MODE | constants.SPLITTING_MODE)

    def print_times_related_to_install_msg(self):
        for sw_id in global_vars.switch_ids:
            self.log.info("test-{0}-sw-{1}-times: {2}\t{3}\t{4}\t{5}".format(self.test_number - 1, sw_id,
                                                                             self.times_sending_to_switches[sw_id] - self.current_start_time,
                                                                             self.times_finishing_receiving_install_msg[sw_id] - self.current_start_time,
                                                                             self.finishing_computation_times[sw_id],
                                                                             self.times_start_receiving_install_msg[sw_id] - self.current_start_time))

    def log_time_using_new_path(self, sw_id):
        for sw_id in self.times_using_new_path_by_switch_strs.keys():
            self.log.info("test-%d-sw-%d-new_path: %s" % ((self.test_number - 1), sw_id,
                                                          self.times_using_new_path_by_switch_strs[sw_id]))

    def store_feedback_from_sw(self, sw_id, data_feedback):
        self.finishing_times[sw_id] = float(data_feedback['finishing_time'])
        self.finishing_computation_times[sw_id] = float(data_feedback['finishing_computation_time'])
        self.times_finishing_receiving_install_msg[sw_id] = float(data_feedback['finish_receiving_time'])
        self.times_start_receiving_install_msg[sw_id] = float(data_feedback['start_receiving_time'])
        self.max_splitting_segments[sw_id] = int(data_feedback['max_splitting_segments'])
        self.max_no_of_working_rules[sw_id] = int(data_feedback['max_working_rules'])
        self.times_using_new_path_by_switch_strs[sw_id] = data_feedback['times_using_new_path_strs']
        self.times_using_new_path_by_switch[sw_id] = data_feedback['times_using_new_path']
        self.times_change_next_sw_by_switch[sw_id] = data_feedback['time_next_sw']
        self.no_of_received_msgs += int(data_feedback['number_msgs'])
        self.overhead_infos[sw_id] = data_feedback['rule_overheads']
        self.receiving_backs[sw_id] = time() * 1000 + global_vars.sw_to_ctrl_delays[sw_id]
        self.msgs_sent_by_sws.update(data_feedback['msgs'])

    def process_feedback(self, data_feedback):
        sw_id = int(data_feedback['switch_id'])
        self.store_feedback_from_sw(sw_id, data_feedback)

        having_deadlock = int(data_feedback['having_deadlock'])
        if self.check_having_splittable_deadlock(having_deadlock) and not self.having_splittable_deadlock:
            self.having_splittable_deadlock = True
        if self.check_having_unsplittable_deadlock(having_deadlock) and not self.having_unsplittable_deadlock:
            self.having_unsplittable_deadlock = True

        global_vars.sent_to_sws.pop(sw_id)
        if len(global_vars.sent_to_sws) == 0:
            time_to_finish_all_update = max(self.finishing_times.values())
            time_to_finish_computation = max(self.finishing_computation_times.values())
            max_splitting_segments = max(self.max_splitting_segments.values())
            max_no_of_working_rules = max(self.max_no_of_working_rules.values())
            finish_all = max(self.receiving_backs.values())
            #count = sum(1 for i in self.max_splitting_segments.values() if i > 0)
            # finish_time = time() * 1000
            finish_time_from_start = time_to_finish_all_update - self.current_start_time
            finish_time_from_sending = time_to_finish_all_update - self.current_sending_time
            finish_time_from_last_sending = time_to_finish_all_update - self.last_sending_time
            finish_all = finish_all - self.last_sending_time + self.current_computation_time
            # self.log.info("finished after %s ms from starting" % finish_time_from_start)
            # self.log.info("finished after %s ms from first sending" % finish_time_from_sending)
            # self.log.info("finished after %s ms" % finish_time_from_last_sending)
            # self.log.info("finished computation after %s ms" % time_to_finish_computation)
            time_to_finish_global = (self.current_sending_time - self.current_start_time)
            time_to_update_only = finish_time_from_last_sending - time_to_finish_computation
            # self.log.info("finished computation global info: %s ms" % time_to_finish_global)

            self.log_time_using_new_path(sw_id)

            for sw_id in self.times_using_new_path_by_switch_strs.keys():
                if self.overhead_infos[sw_id] != "":
                    self.log.info("test-%d-sw-%d-split: %s" % ((self.test_number - 1), sw_id,
                                                               self.overhead_infos[sw_id]))

            for sw_id in self.max_no_of_working_rules.keys():
                self.log.info("test-%d-sw-%d-no_rules: %d" % ((self.test_number - 1), sw_id,
                                                             self.max_no_of_working_rules[sw_id]))

            self.print_times_related_to_install_msg()

            self.log.info("test-%d: %s\t%s\t%s\t%f\t%d\t%d\t%s\t%d\t%s\t%s\t%s\t%s" %
                          ((self.test_number - 1), finish_time_from_start, time_to_finish_computation,
                           time_to_finish_global, time_to_update_only,
                           max_splitting_segments, max_no_of_working_rules, self.having_splittable_deadlock,
                           self.no_of_received_msgs, finish_time_from_sending, finish_time_from_last_sending,
                           self.having_unsplittable_deadlock, finish_all))
            msgs_sent_by_sws = {}
            for key in self.msgs_sent_by_sws.keys():
                msgs_sent_by_sws[key] = self.msgs_sent_by_sws[key]
            self.log.info("test-%d-msgs: %s" % ((self.test_number - 1), str(msgs_sent_by_sws)))
            self.msgs_sent_by_sws = OrderedDict(sorted(msgs_sent_by_sws.items()))
            event_list = self.create_event_list(self.msgs_sent_by_sws, self.times_using_new_path_by_switch_strs)
            self.dump_list_msgs_to_file(event_list)

            if self.having_splittable_deadlock:
                self.deadlock_count += 1
                self.log.info("deadlock count %d" % self.deadlock_count)
            if self.having_unsplittable_deadlock:
                self.deadlock_unsplittable_count += 1
                self.log.info("unsplittable deadlock count %d" % self.deadlock_unsplittable_count)

            # self.log.info("finished after %s: " % str(max(self.finishing_times.values())))
            # self.on_timer()
            eventlet.spawn_after(1.5, self.on_timer)

    def create_event_list(self, msgs_sent_by_sws, time_using_new_path):
        event_list = {}
        for key in msgs_sent_by_sws.keys():
            for msg in msgs_sent_by_sws[key]:
                new_event = MessageSendingEvent(msg.src_id, msg.dst_id, key, msg.msg_type,
                                                msg.receiving_time, msg.seg_path_id, msg.split_vol)
                if event_list.has_key(key):
                    event_list[key].append(new_event)
                else:
                    event_list[key] = [new_event]
        for sw in self.times_using_new_path_by_switch.keys():
            for key in self.times_using_new_path_by_switch[sw].keys():
                value = self.times_using_new_path_by_switch[sw][key]
                new_event = FlowChangeEvent(sw, value, key)
                if event_list.has_key(value):
                    event_list[value].append(new_event)
                else:
                    event_list[value] = [new_event]
            for key2 in self.times_change_next_sw_by_switch[sw].keys():
                time_next_sw, next_sw, type = self.times_change_next_sw_by_switch[sw][key2]
                new_event = FlowTableChangeEvent(sw, time_next_sw, key2, next_sw, type)
                if event_list.has_key(time_next_sw):
                    event_list[time_next_sw].append(new_event)
                else:
                    event_list[time_next_sw] = [new_event]

        ordered_list = OrderedDict(sorted(event_list.items()))
        return ordered_list

    def dump_list_msgs_to_file(self, event_list):
        of = open("../logs/message_log.json", "w")
        pickle_msgs = pickle.dumps(event_list, pickle.HIGHEST_PROTOCOL)
        of.write(pickle_msgs)
        of.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ez-segway sim.')
    parser.add_argument('--simulationDuration', nargs='?',
                        type=int, default=5000)
    parser.add_argument('--logFolder', nargs='?',
                        type=str, default="logs")
    parser.add_argument('--logLevel', nargs='?',
                        type=str, default="INFO")
    parser.add_argument('--logFile', nargs='?',
                        type=str, default="stdout")
    parser.add_argument('--data_folder', nargs='?',
                        type=str, default="data")
    parser.add_argument('--topology', nargs='?',
                        type=str, default="triangle")
    parser.add_argument('--topology_type', nargs='?',
                        type=str, default=constants.TOPO_ADJACENCY)
    parser.add_argument('--method', nargs='?',
                        type=str, default=constants.P2P_METHOD)
    parser.add_argument('--generating_method', nargs='?',
                        type=str, default=constants.LINK_FAILURE_GENERATION)
    parser.add_argument('--number_of_flows', nargs='?',
                        type=int, default=2)
    parser.add_argument('--failure_rate', nargs='?',
                        type=float, default=0.5)
    parser.add_argument('--repeat_time', nargs='?',
                        type=int, default=9)
    parser.add_argument('--skip_deadlock', nargs='?',
                        type=int, default=0)
    args = parser.parse_args()

    directory = "../%s" % (args.logFolder)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # constants.LOG_LEVEL = args.logLevel

    logger.init("../" + args.logFolder + "/" + args.logFile, constants.LOG_LEVEL)
    log = logger.getLogger("ez-segway", constants.LOG_LEVEL)
    #log.info("---> Log start <---")
    #log.info(args)
    log.level = constants.LOG_LEVEL
    ctrl = EzGlobalController(log, args.repeat_time, args.skip_deadlock)
    ctrl.run_experiment(args)
