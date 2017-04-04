"""Custom topology example

Two directly connected switches plus a host for each switch:

   host --- switch --- switch --- host

Adding the 'topos' dict with a key/value pair to generate our newly defined
topology enables one to pass in '--topo=mytopo' from the command line.
"""

from basetopo import BaseTopo


class Triangle(BaseTopo):
    "Simple topology ex."

    def __init__( self ):
        "Create custom topo."

        # Initialize topology
        BaseTopo.__init__( self )

        # Add hosts and switches
        leftHost = self.addHost('h1')
        rightHost = self.addHost('h2')
        s1 = self.addSwitch(BaseTopo.get_switch_name(1))
        s2 = self.addSwitch(BaseTopo.get_switch_name(2))
        s3 = self.addSwitch(BaseTopo.get_switch_name(3))

        # Add links
        self.addLink(leftHost, s1)
        self.addLink(rightHost, s2)
        self.addLink(s1, s2, delay='5.09ms', loss=0)
        self.addLink(s1, s3, delay='1.12ms', loss=0)
        self.addLink(s2, s3, delay='3.63ms', loss=0)
