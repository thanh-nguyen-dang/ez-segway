from basetopo import BaseTopo


class Internet2(BaseTopo):
    "Simple topology ex."

    def __init__( self ):
        "Create custom topo."

        # Initialize topology
        BaseTopo.__init__(self)

        # Add hosts and switches
        hs = [None]
        ss = [None]
        for i in xrange(1, 17):
            hs.append(self.addHost(BaseTopo.get_host_name(i)))
            ss.append(self.addSwitch(BaseTopo.get_switch_name(i)))

        # Add links
        for i in xrange(1, 17):
            self.addLink(hs[i], ss[i])
        self.addLink(ss[1], ss[2], delay='5.69ms', loss=0)
        self.addLink(ss[1], ss[4], delay='5.55ms', loss=0)
        self.addLink(ss[1], ss[11], delay='17.32ms', loss=0)#ss[13])
        self.addLink(ss[2], ss[3], delay='12.63ms', loss=0)
        self.addLink(ss[2], ss[4], delay='4.68ms', loss=0)
        self.addLink(ss[3], ss[4], delay='10.08ms', loss=0)
        self.addLink(ss[3], ss[6], delay='12.66ms', loss=0)
        self.addLink(ss[4], ss[5], delay='5.83ms', loss=0)
        self.addLink(ss[5], ss[6], delay='8.76ms', loss=0)
        self.addLink(ss[5], ss[7], delay='4.65ms', loss=0)
        self.addLink(ss[6], ss[8], delay='12.67ms', loss=0)
        self.addLink(ss[7], ss[8], delay='2.39ms', loss=0)
        self.addLink(ss[7], ss[10], delay='7.32ms', loss=0)
        self.addLink(ss[8], ss[9], delay='2.76ms', loss=0)
        self.addLink(ss[8], ss[16], delay='6.54ms', loss=0)#ss[18])
        self.addLink(ss[9], ss[10], delay='3.42ms', loss=0)
        self.addLink(ss[9], ss[15], delay='8.03ms', loss=0)#ss[17])
        self.addLink(ss[10], ss[11], delay='4.45ms', loss=0)
        #self.addLink(ss[10], ss[12])
        #self.addLink(ss[11], ss[12])
        #self.addLink(ss[11], ss[14])
        #self.addLink(ss[12], ss[13])
        self.addLink(ss[10], ss[12], delay='3.66ms', loss=0)
        # self.addLink(ss[10], ss[11], delay='0.91ms', loss=0)
        # self.addLink(ss[14], ss[15])
        # self.addLink(ss[14], ss[16])
        # self.addLink(ss[15], ss[16])
        # self.addLink(ss[16], ss[17])
        # self.addLink(ss[17], ss[18])
        self.addLink(ss[12], ss[13], delay='1.46ms', loss=0)
        self.addLink(ss[12], ss[14], delay='2.55ms', loss=0)
        self.addLink(ss[13], ss[14], delay='2.69ms', loss=0)
        self.addLink(ss[14], ss[15], delay='3.78ms', loss=0)
        self.addLink(ss[15], ss[16], delay='5.24ms', loss=0)
