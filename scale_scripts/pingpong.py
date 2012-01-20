import socket
import sys
from threading import Timer
import threading
from dashi import bootstrap
import datetime
import simplejson as json

class DashiScalePonger(object):

    def __init__(self, CFG):
        self.CFG = CFG
        self.dashi = bootstrap.dashi_connect(CFG.test.ponger_name, CFG)
        self.done = False
        self.dashi.handle(self.ping, "ping")
        self.dashi.handle(self.final_msg, "final_msg")

    def ping(self):
        self.dashi.fire(self.CFG.test.pinger_name, "pong")

    def final_msg(self):
        print "received final message"
        self.done = True
        sys.exit(0)

    def go(self):
        while not self.done:
            try:
                self.dashi.consume()
            except socket.timeout, ex:
                pass


class DashiScalePinger(object):

    def __init__(self, CFG):
        self.CFG = CFG
        self.dashi = bootstrap.dashi_connect(CFG.test.pinger_name, CFG)
        self.done = False
        self.end_time = None
        self.start_time = None
        self.message_count = 0
        self.dashi.handle(self.pong, "pong")
        self.timer = Timer(float(CFG.test.runtime), self.timeout)

    def go(self):
        self.timer.start()
        self.start_time = datetime.datetime.now()
        print "sending first ping"
        self.dashi.fire(self.CFG.test.ponger_name, "ping")
        while not self.done:
            try:
                self.dashi.consume(count=1, timeout=2)
            except socket.timeout, ex:
                pass
        print "sending final message"
        self.dashi.fire(self.CFG.test.ponger_name, "final_msg")

    def timeout(self):
        self.end_time = datetime.datetime.now()
        self.done = True

    def pong(self):
        self.message_count = self.message_count + 1
        self.dashi.fire(self.CFG.test.ponger_name, "ping")

    def get_results(self):
        tm = self.end_time - self.start_time
        preci = float(tm.microseconds) / 1000000.0
        tm = tm.seconds + preci
        res = {}
        res['testname'] = self.CFG.test.name
        res['message_count'] = self.message_count
        res['runtime'] = tm
        res['process_type'] = "pinger"

        return res

def main(argv):
    CFG = bootstrap.configure(argv=argv)

    if CFG.test.type == "ping":
        sender = DashiScalePinger(CFG)
        sender.go()
        res = sender.get_results()
        print "JSON: %s" % (json.dumps(res))
    else:
        receiver = DashiScalePonger(CFG)
        receiver.go()


        
if __name__ == '__main__':
    rc = main(sys.argv)
    print "exit"
    sys.exit(rc)
