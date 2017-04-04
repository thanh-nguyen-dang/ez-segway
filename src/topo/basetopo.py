from mininet.topo import Topo
from collections import defaultdict


class BaseTopo( Topo ):

    def extract_topo(self):
        t = defaultdict(dict)
        for link in self.iterLinks(withKeys=True, withInfo=True):
            src, dst, key, info = link
            if self.isSwitch(src) and self.isSwitch(dst):
                s = int(src[1:])
                d = int(dst[1:])
                t[s][d] = info['port1']
                t[d][s] = info['port2']
        return t

    def extract_topo_latency(self):
        pass

    @staticmethod
    def get_switch_name(i):
        return "s%d" % i

    @staticmethod
    def get_controller_name(i):
        return "c%d" % i

    @staticmethod
    def get_host_name(i):
        return "h%d" % i
