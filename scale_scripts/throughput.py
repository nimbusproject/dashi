import socket
import sys
from threading import Timer
import threading
from dashi import bootstrap
import datetime
import simplejson as json

def build_message(ent_count, ent_size):
    ent = ""
    for i in range(0, ent_size):
        ent = ent + "X"
    #s = bytearray(ent)
    d= {}
    for i in range(0, ent_count):
        d[i] = ent
    return ent


class DashiScaleReceiver(object):

    def __init__(self, CFG):
        self.CFG = CFG
        self.dashi = bootstrap.dashi_connect(CFG.test.receiver_name, CFG)
        self.done = False
        self.end_time = None
        self.start_time = None
        self.dashi.handle(self.incoming, "incoming")
        self.dashi.handle(self.final_msg, "final_msg")
        self.message_count = 0

    def incoming(self, message):
        self.message_count = self.message_count + 1
        if self.start_time is None:
            self.start_time = datetime.datetime.now()

    def final_msg(self):
        self.end_time = datetime.datetime.now()
        self.done = True
        print "got final message"

    def go(self):
        while not self.done:
            try:
                self.dashi.consume(timeout=int(self.CFG.test.consume_timeout), count=int(self.CFG.test.consume_count))
            except socket.timeout, ex:
                pass

    def get_results(self):
        tm = self.end_time - self.start_time
        preci = float(tm.microseconds) / 1000000.0
        tm = tm.seconds + preci
        res = {}
        res['testname'] = self.CFG.test.name
        res['message_count'] = self.message_count
        res['entry_size'] = self.CFG.test.message.entry_size
        res['entry_count'] = self.CFG.test.message.entry_count
        res['runtime'] = tm
        res['process_type'] = "receiver"

        return res

class DashiScaleSender(object):

    def __init__(self, CFG):
        self.CFG = CFG
        self.dashi = bootstrap.dashi_connect(CFG.test.sender_name, CFG)
        self.done = False
        self.timer = Timer(float(CFG.test.runtime), self.timeout)
        self.end_time = None
        self.start_time = None
        self.message_count = 0
        self.message = build_message(int(self.CFG.test.message.entry_size), int(self.CFG.test.message.entry_count))

    def go(self):
        self.start_time = datetime.datetime.now()
        self.timer.start()
        while not self.done:
            if not self.done:
                self.dashi.fire(self.CFG.test.receiver_name, "incoming", message=self.message)
                self.message_count = self.message_count + 1
        print "sending final message"
        self.dashi.fire(self.CFG.test.receiver_name, "final_msg")

    def timeout(self):
        self.end_time = datetime.datetime.now()
        self.done = True

    def get_results(self):
        tm = self.end_time - self.start_time
        preci = float(tm.microseconds) / 1000000.0
        tm = tm.seconds + preci
        res = {}
        res['testname'] = self.CFG.test.name
        res['message_count'] = self.message_count
        res['entry_size'] = self.CFG.test.message.entry_size
        res['entry_count'] = self.CFG.test.message.entry_count
        res['runtime'] = tm
        res['process_type'] = "sender"

        return res

def main(argv):
    CFG = bootstrap.configure(argv=argv)

    if CFG.test.type == "S":
        sender = DashiScaleSender(CFG)
        sender.go()
        res = sender.get_results()
    else:
        receiver = DashiScaleReceiver(CFG)
        receiver.go()
        res = receiver.get_results()

    print "JSON: %s" % (json.dumps(res))
        
if __name__ == '__main__':
    rc = main(sys.argv)
    print "exit"
    sys.exit(rc)
