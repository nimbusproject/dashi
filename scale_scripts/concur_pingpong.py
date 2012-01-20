import socket
import sys
from threading import Timer
from threading import Thread
from dashi import bootstrap
import datetime
import simplejson as json

class DashiConcurScalePonger(object):

    def __init__(self, CFG):
        self.CFG = CFG
        self.dashi = bootstrap.dashi_connect(CFG.test.ponger_name, CFG)
        self.done = False
        self.dashi.handle(self.ping, "ping")
        self.dashi.handle(self.final_msg, "final_msg")

    def ping(self, from_name=None):
        #print "ponging to %s" % (from_name)
        self.dashi.fire(from_name, "pong")

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


class DashiConcurScalePinger(Thread):

    def __init__(self, CFG, cnt):
        Thread.__init__(self)
        self._myname = CFG.test.pinger_name + "_" + str(cnt)
        self.CFG = CFG
        self.dashi = bootstrap.dashi_connect(self._myname, CFG)
        self.done = False
        self.end_time = None
        self.start_time = None
        self.message_count = 0
        self.dashi.handle(self.pong, "pong")
        self.timer = Timer(float(CFG.test.runtime), self.timeout)

    def run(self):
        self.go()

    def go(self):
        self.timer.start()
        #print "sending first ping to %s" % (self.CFG.test.ponger_name)
        self.dashi.fire(self.CFG.test.ponger_name, "ping", from_name=self._myname)
        while not self.done:
            try:
                self.dashi.consume(count=1, timeout=10)
            except socket.timeout, ex:
                pass

    def timeout(self):
        self.end_time = datetime.datetime.now()
        self.done = True

    def pong(self):
        #print "got pong"
        self.message_count = self.message_count + 1
        self.dashi.fire(self.CFG.test.ponger_name, "ping", from_name=self._myname)


def main(argv):
    CFG = bootstrap.configure(argv=argv)

    if CFG.test.type == "ping":
        sender_count = int(CFG.test.concur)
        print "sender count %d" % (sender_count)
        thrs = []
        start_time = datetime.datetime.now()
        for i in range(0, sender_count):
            sender = DashiConcurScalePinger(CFG, i)
            thrs.append(sender)
            sender.start()

        msg_count_total = 0
        for t in thrs:
            t.join()
            msg_count_total = msg_count_total + t.message_count
        end_time = datetime.datetime.now()

        tm = end_time - start_time
        preci = float(tm.microseconds) / 1000000.0
        runtime = tm.seconds + preci

        res = {}
        res['testname'] = "concurtest"
        res['message_count'] = msg_count_total
        res['runtime'] = runtime
        res['process_type'] = "pinger"
        res['connection_count'] = len(thrs)
        print "JSON: %s" % (json.dumps(res))
    else:
        print "ponger go"
        receiver = DashiConcurScalePonger(CFG)
        receiver.go()


        
if __name__ == '__main__':
    print "start"
    rc = main(sys.argv)
    print "exit"
    sys.exit(rc)
