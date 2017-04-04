from misc import constants

class OverheadInfo(object):
    def __init__(self, total_no_of_flows, no_of_splits):
        self.total_no_of_flows = total_no_of_flows
        self.no_of_splits = no_of_splits

    def __str__(self):
        return "%d: %d" % (self.no_of_splits, self.total_no_of_flows)

    def __repr__(self):
        return self.__str__()

class ExecutionInfo(object):
    def __init__(self):
        self.max_splitting_segments = 0
        self.no_segments_splitting = 0
        self.max_no_of_working_rules = 0
        self.no_of_splitting_by_time = []
        self.time_using_new_path_by_seg_path_id = {}
        self.time_new_next_sw_by_seg_path_id = {}
        self.no_of_working_rules = 0
        self.no_of_received_messages = 0
        self.list_overhead_infos = []
        self.list_msgs_with_sending_time = {}

    def reset(self):
        self.max_splitting_segments = 0
        self.no_segments_splitting = 0
        self.max_no_of_working_rules = 0
        self.no_of_splitting_by_time = []
        self.time_using_new_path_by_seg_path_id = {}
        self.time_new_next_sw_by_seg_path_id = {}
        self.no_of_working_rules = 0
        self.no_of_received_messages = 0
        self.list_overhead_infos = []
        self.list_msgs_with_sending_time = {}

    def __str__(self):
        return "%s\t%s\t%s" % (self.max_splitting_segments, self.max_no_of_working_rules,
                               self.times_using_new_path_to_string())

    def __repr__(self):
        return self.__str__()

    def times_using_new_path_to_string(self):
        str_times_using_new_path = "["
        for key in self.time_using_new_path_by_seg_path_id.keys():
            str_times_using_new_path += "%s:%s%s" % (key, self.time_using_new_path_by_seg_path_id[key],
                                                     constants.TIME_USING_NEW_PATH_SEPARATOR)
        if len(self.time_using_new_path_by_seg_path_id) > 0:
            str_times_using_new_path = str_times_using_new_path[:-1]
        str_times_using_new_path += "]"
        return str_times_using_new_path

    def add_trace_for_msgs(self, list):
        for msg in list:
            if self.list_msgs_with_sending_time.has_key(msg.sending_time):
                self.list_msgs_with_sending_time[msg.sending_time].append(msg)
            else:
                self.list_msgs_with_sending_time[msg.sending_time] = [msg]
        # self.list_msgs_with_sending_time[time] = list

    def msgs_to_string(self):
        str_msgs_with_times = "["
        for key in self.list_msgs_with_sending_time.keys():
            str_msgs_with_times += "%s:%s%s" % (key, self.list_msgs_with_sending_time[key],
                                                     constants.TIME_USING_NEW_PATH_SEPARATOR)
        if len(self.time_using_new_path_by_seg_path_id) > 0:
            str_msgs_with_times = str_msgs_with_times[:-1]
        str_msgs_with_times += "]"
        return str_msgs_with_times

    def list_to_string(self, list):
        if len(list) == 0:
            return ""
        return_str = "["
        for item in list:
            return_str += "%s," % item
        return_str = return_str[:-1]
        return_str += "]"
        return return_str

    def convert_to_time_from_starting(self, starting_time, computation_time):
        for key in self.time_using_new_path_by_seg_path_id.keys():
            self.time_using_new_path_by_seg_path_id[key] = self.time_using_new_path_by_seg_path_id[key]\
                                                           - starting_time + computation_time
        list_msgs_with_new_sending_times = {}
        for key in self.list_msgs_with_sending_time.keys():
            new_time = key - starting_time
            list_msgs = self.list_msgs_with_sending_time[key]
            for msg in list_msgs:
                msg.sending_time = new_time
                msg.receiving_time = msg.receiving_time - starting_time
            list_msgs_with_new_sending_times[new_time] = list_msgs

        for key in self.time_new_next_sw_by_seg_path_id.keys():
            time_next_sw, next_sw, type = self.time_new_next_sw_by_seg_path_id[key]
            time_next_sw -= starting_time
            self.time_new_next_sw_by_seg_path_id[key] = (time_next_sw, next_sw, type)

        self.list_msgs_with_sending_time.clear()
        self.list_msgs_with_sending_time = list_msgs_with_new_sending_times


    def update_overhead_info(self):
        self.list_overhead_infos.append(OverheadInfo(self.no_of_working_rules, self.no_segments_splitting))
