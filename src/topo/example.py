from basetopo import BaseTopo


class Example(BaseTopo):
    "Simple topology ex."

    def __init__( self ):
        "Create custom topo."

        # Initialize topology
        BaseTopo.__init__( self )

        # Add hosts and switches
        hs = [None]
        ss = [None]
        for i in xrange(1, 8):
            hs.append(self.addHost(BaseTopo.get_host_name(i)))
            ss.append(self.addSwitch(BaseTopo.get_switch_name(i)))

        # Add links
        for i in xrange(1, 8):
            self.addLink(hs[i], ss[i])
        self.addLink(ss[1], ss[2], delay='1ms', loss=0)
        self.addLink(ss[2], ss[3], delay='1ms', loss=0)
        self.addLink(ss[2], ss[6], delay='1ms', loss=0)
        self.addLink(ss[3], ss[4], delay='1ms', loss=0)
        self.addLink(ss[3], ss[6], delay='1ms', loss=0)
        self.addLink(ss[3], ss[7], delay='1ms', loss=0)
        self.addLink(ss[4], ss[5], delay='1ms', loss=0)
        self.addLink(ss[4], ss[7], delay='1ms', loss=0)
