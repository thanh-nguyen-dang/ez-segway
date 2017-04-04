from basetopo import BaseTopo


class rf_6462(BaseTopo):

    def __init__( self ):
        "Create custom topo."

        # Initialize topology
        BaseTopo.__init__( self )

        # Add hosts and switches
        hs = [None]
        ss = [None]
        # node_count = len(rf_topo.graph.nodes())
        for i in xrange(1, 23):
            hs.append(self.addHost(BaseTopo.get_host_name(i)))
            ss.append(self.addSwitch(BaseTopo.get_switch_name(i)))

        # Add links
        for i in xrange(1, 23):
            self.addLink(hs[i], ss[i])

        self.addLink(ss[11], ss[18], delay='2ms', loss=0)
        self.addLink(ss[19], ss[20], delay='2ms', loss=0)
        self.addLink(ss[6], ss[9], delay='11ms', loss=0)
        self.addLink(ss[7], ss[8], delay='3ms', loss=0)
        self.addLink(ss[7], ss[11], delay='21ms', loss=0)
        self.addLink(ss[11], ss[19], delay='2ms', loss=0)
        self.addLink(ss[5], ss[6], delay='7ms', loss=0)
        self.addLink(ss[12], ss[22], delay='40ms', loss=0)
        self.addLink(ss[1], ss[2], delay='3ms', loss=0)
        self.addLink(ss[2], ss[13], delay='5ms', loss=0)
        self.addLink(ss[8], ss[9], delay='3ms', loss=0)
        self.addLink(ss[9], ss[10], delay='6ms', loss=0)
        self.addLink(ss[3], ss[13], delay='6ms', loss=0)
        self.addLink(ss[7], ss[15], delay='3ms', loss=0)
        self.addLink(ss[21], ss[22], delay='40ms', loss=0)
        self.addLink(ss[11], ss[20], delay='2ms', loss=0)
        self.addLink(ss[3], ss[8], delay='29ms', loss=0)
        self.addLink(ss[6], ss[11], delay='13ms', loss=0)
        self.addLink(ss[5], ss[7], delay='6ms', loss=0)
        self.addLink(ss[11], ss[21], delay='2ms', loss=0)
        self.addLink(ss[1], ss[3], delay='3ms', loss=0)
        self.addLink(ss[2], ss[4], delay='4ms', loss=0)
        self.addLink(ss[9], ss[14], delay='37ms', loss=0)
        self.addLink(ss[5], ss[9], delay='6ms', loss=0)
        self.addLink(ss[3], ss[9], delay='31ms', loss=0)
        self.addLink(ss[11], ss[16], delay='20ms', loss=0)
        self.addLink(ss[8], ss[12], delay='21ms', loss=0)
        self.addLink(ss[9], ss[11], delay='21ms', loss=0)
        self.addLink(ss[10], ss[12], delay='15ms', loss=0)
        self.addLink(ss[3], ss[4], delay='3ms', loss=0)
        self.addLink(ss[7], ss[9], delay='2ms', loss=0)
        self.addLink(ss[11], ss[17], delay='4ms', loss=0)
        self.addLink(ss[6], ss[10], delay='8ms', loss=0)
        self.addLink(ss[5], ss[8], delay='8ms', loss=0)
        self.addLink(ss[11], ss[12], delay='7ms', loss=0)
        self.addLink(ss[1], ss[4], delay='4ms', loss=0)
        self.addLink(ss[9], ss[15], delay='3ms', loss=0)
        self.addLink(ss[2], ss[14], delay='5ms', loss=0)
        self.addLink(ss[9], ss[12], delay='20ms', loss=0)
        self.addLink(ss[3], ss[11], delay='45ms', loss=0)
        self.addLink(ss[10], ss[11], delay='16ms', loss=0)
        self.addLink(ss[8], ss[10], delay='7ms', loss=0)
        self.addLink(ss[4], ss[9], delay='32ms', loss=0)
