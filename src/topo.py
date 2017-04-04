#!/usr/bin/python

"""
Create a network where different switches are connected to
different controllers, by creating a custom Switch() subclass.
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, Controller, RemoteController
from mininet.topolib import TreeTopo
from mininet.log import setLogLevel
from mininet.cli import CLI
from mininet.link import TCLink
from topo.topo_factory import TopoFactory
from topo.basetopo import BaseTopo
import argparse

setLogLevel( 'info' )
cmap = {}


class MultiSwitch( OVSSwitch ):
    def __init__(self, name, **params):
        params['protocols'] = 'OpenFlow13,OpenFlow10'
        OVSSwitch.__init__(self, name, **params)

    "Custom Switch() subclass that connects to different controllers"
    def start( self, controllers):
        return OVSSwitch.start(self, [ cmap[ self.name ] ])


def createP2PControllers(net, switches):
    cs = {}
    for i in xrange(1, len(switches) + 1):
        c = RemoteController(BaseTopo.get_controller_name(i), ip='127.0.0.1', port=6732 + i)
        net.addController(c)
        cs[BaseTopo.get_switch_name(i)] = c
    return cs


def createCentralControllers(net, switches):
    cs = {}
    for i in xrange(1, len(switches) + 1):
        ip_s = '127.0.0.%d' % (i + 1)
        print ip_s
        c = RemoteController(BaseTopo.get_controller_name(i), ip=ip_s, port=6733)
        net.addController(c)
        cs[BaseTopo.get_switch_name(i)] = c
    return cs


def create(method, topo_name):
    topo = TopoFactory.create_topo(topo_name)
    net = Mininet( topo=topo, switch=MultiSwitch, link=TCLink, build=False, autoStaticArp=True )
    if method == "p2p":
        return net, createP2PControllers(net, topo.switches())
    elif method == "central":
        return net, createCentralControllers(net, topo.switches())

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ctrl')
    parser.add_argument('--method', nargs='?',
                        type=str, default="p2p")
    parser.add_argument('--topo', nargs='?',
                        type=str, default="triangle")
    args = parser.parse_args()

    net, cmap = create(args.method, args.topo)
    net.build()
    net.start()
    # execute iPerl command according to the flow that is generated
    # monitoring packet loss
    CLI( net )
    net.stop()
