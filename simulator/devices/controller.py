import random

from misc import global_vars, utils, message_utils, logger
from misc.message_utils import *
from domain.message import *


class Controller(Simulation.Process):
    def __init__(self):
        self.log = self.init_logger()
        self.log.debug("Create Controller")

        self.pending_msgs = []
        self.pending_evt = Simulation.SimEvent("Pending-Event")

        self.finished_update_evt = Simulation.SimEvent("Finished-Event")
        self.current_update = -1
        self.skip_deadlock = False

        Simulation.Process.__init__(self, name='Controller')

    def __str__(self):
        return "Controller"

    @staticmethod
    def init_logger():
        return logger.getLogger("Controller", constants.LOG_LEVEL)

    def recv(self, msg):
        self.pending_msgs.append(msg)
        self.pending_evt.signal()

    def send_to_switch(self, msg, dst_id):
        switch = utils.get_item_with_lambda(global_vars.switches, lambda item: dst_id - item.id)
        self.send(msg, switch)

    def send(self, msg, dst):
        debug = self.log.debug

        delay = global_vars.sw_to_ctrl_delays[dst.id] + \
            random.normalvariate(constants.NW_LATENCY_MU,
                                 constants.NW_LATENCY_SIGMA)
        debug("sending msg %s from controller to %s @:%f, delay time: %f" % (msg, dst, Simulation.now(), delay))
        deliver = DeliverMessageWithDelay()
        Simulation.activate(deliver,
                            deliver.run(msg, delay, dst),
                            at=Simulation.now())

    def install_update(self, old_flows, new_flows):
        pass

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
                if isinstance(msg, NotificationMessage):
                    self.log.debug("yield from queue_event msg from %d %s" \
                                   % (msg.src_id, msg))
                    self.handle_notification(msg)
                self.pending_msgs.remove(msg)

            # TODO: wait on a signal

    def handle_notification(self, msg):
        pass