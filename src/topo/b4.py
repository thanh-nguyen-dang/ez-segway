from basetopo import BaseTopo


class B4(BaseTopo):
    "Simple topology ex."

    def __init__( self ):
        "Create custom topo."

        # Initialize topology
        BaseTopo.__init__( self )

        # Add hosts and switches
        hs = [None]
        ss = [None]
        for i in xrange(1, 13):
            hs.append(self.addHost(BaseTopo.get_host_name(i)))
            ss.append(self.addSwitch(BaseTopo.get_switch_name(i)))

        # Add links
        for i in xrange(1, 13):
            self.addLink(hs[i], ss[i])
        self.addLink(ss[1], ss[2], delay='3.98ms', loss=0)
        self.addLink(ss[1], ss[3], delay='52.1ms', loss=0)
        self.addLink(ss[2], ss[5], delay='57.33ms', loss=0)
        self.addLink(ss[3], ss[4], delay='16ms', loss=0)
        self.addLink(ss[3], ss[6], delay='13.93ms', loss=0)
        self.addLink(ss[4], ss[5], delay='5.77ms', loss=0)
        self.addLink(ss[4], ss[7], delay='14.74ms', loss=0)
        self.addLink(ss[4], ss[8], delay='2.39ms', loss=0)
        self.addLink(ss[5], ss[6], delay='7.44ms', loss=0)
        self.addLink(ss[6], ss[7], delay='10.82ms', loss=0)
        self.addLink(ss[6], ss[8], delay='8.89ms', loss=0)
        self.addLink(ss[7], ss[8], delay='17.01ms', loss=0)
        self.addLink(ss[7], ss[11], delay='41.82ms', loss=0)
        self.addLink(ss[8], ss[10], delay='33.95ms', loss=0)
        self.addLink(ss[9], ss[10], delay='4.76ms', loss=0)
        self.addLink(ss[9], ss[11], delay='5.07ms', loss=0)
        self.addLink(ss[10], ss[11], delay='1.87ms', loss=0)
        self.addLink(ss[10], ss[12], delay='9.61ms', loss=0)
        self.addLink(ss[11], ss[12], delay='7.86ms', loss=0)
