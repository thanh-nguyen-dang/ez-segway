import random

from domain.message import *
from misc import global_vars, utils, message_utils, logger
from misc.message_utils import *


class Switch(Simulation.Process):
    def __init__(self, id_, ctrl, neighbor_ids):
        self.id = id_
        self.log = self.init_logger(id_)

        self.ctrl = ctrl
        self.neighbor_ids = neighbor_ids

        self.pending_msgs = []
        self.pending_evt = Simulation.SimEvent("Pending-Event-%s" % id_)

        Simulation.Process.__init__(self, name='Switch' + str(id_))

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.id < other.id

    def __le__(self, other):
        return not other < self

    def __ge__(self, other):
        return not self < other

    def __str__(self):
        return "Switch%d" % self.id

    @staticmethod
    def init_logger(id_):
        return logger.getLogger("Switch%d" % id_, constants.LOG_LEVEL)

    def recv(self, msg):
        self.pending_msgs.append(msg)
        self.pending_evt.signal()

    def send_to_ctrl(self, msg):
        debug = self.log.debug

        delay = global_vars.sw_to_ctrl_delays[self.id] + \
            random.normalvariate(constants.NW_LATENCY_MU,
                                 constants.NW_LATENCY_SIGMA)
        debug("sending msg %s to %s @:%f, delay time: %f" % (msg, self.ctrl, Simulation.now(), delay))
        deliver = DeliverMessageWithDelay()
        Simulation.activate(deliver,
                            deliver.run(msg, delay, self.ctrl),
                            at=Simulation.now())

    def send_to_switch(self, msg, dst_id):
        debug = self.log.debug
        dst_sw = utils.get_item_with_lambda(global_vars.switches, lambda item: dst_id - item.id)
        delay = global_vars.sw_to_sw_delays[(self.id, dst_sw.id)] + \
                random.normalvariate(constants.NW_LATENCY_MU,
                                     constants.NW_LATENCY_SIGMA)
        debug("sending msg %s to %s @:%f, delay: %f" % (msg, dst_sw, Simulation.now(), delay))
        deliver = DeliverMessageWithDelay()
        Simulation.activate(deliver,
                            deliver.run(msg, delay, dst_sw),
                            at=Simulation.now())

    def run(self):
        debug = self.log.debug
        info = self.log.info
        warning = self.log.warning
        critical = self.log.critical

        while True:
            yield Simulation.queueevent, self, [self.pending_evt]

            while len(self.pending_msgs) > 0:
                msg = self.pending_msgs[0]
                # for msg in self.pending_msgs:
                if isinstance(msg, InstallUpdateMessage):
                    self.log.debug("yield from queue_event %s" % msg)
                    self.install_update(msg)
                elif isinstance(msg, NotificationMessage):
                    self.log.debug("yield from queue_event msg from %d %s" \
                                   % (msg.src_id, msg))
                    self.handle_notification(msg)
                self.pending_msgs.remove(msg)

    def install_update(self, msg):
        pass

    # Handling all notification messages
    def handle_notification(self, msg):
        pass
