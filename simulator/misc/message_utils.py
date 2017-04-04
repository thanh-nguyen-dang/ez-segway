import SimPy.Simulation as Simulation

class DeliverMessageWithDelay(Simulation.Process):
    def __init__(self):
        Simulation.Process.__init__(self, name='DeliverMessageWithDelay')

    def run(self, msg, delay, dst):
        yield Simulation.hold, self, delay
        msg.deliverTime = Simulation.now()
        dst.recv(msg)
