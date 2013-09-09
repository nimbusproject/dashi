#!/usr/bin/env python

import socket
import sys
from threading import Thread
from dashi import DashiConnection

g_rabbit_url = ""


class TalkConsole(object):

    def __init__(self):
        self._prompt = ">> "

    def write(self, msg):
        sys.stdout.write("\r%s" % (msg))
        sys.stdout.write("\n" + self._prompt)
        sys.stdout.flush()

    def input(self):
        line = raw_input(self._prompt)
        return line.strip()


class DashiTalker(Thread):

    def __init__(self, console, name):
        Thread.__init__(self)
        self.name = name
        self.done = False
        self.exchange = "default_dashi_exchange"
        global g_rabbit_url
        self.dashi = DashiConnection(self.name, g_rabbit_url, self.exchange, ssl=True)
        self.subscribers = []
        self.console = console
        self.dashi.handle(self.new_joined_chat, "new_joined_chat")
        self.dashi.handle(self.incoming_message, "incoming_message")

    def new_joined_chat(self, subscriber):
        self.subscribers.append(subscriber)
        self.console.write("%s has entered the room" % (subscriber))
        return True

    def input_message(self, msg, sender_name=None):
        if sender_name:
            msg = "%s said: %s" % (sender_name, msg)
        for subscriber in self.subscribers:
            self.dashi.fire(subscriber, "incoming_message", message=msg)

    def request_conversation(self, with_who):
        rc = self.dashi.call(with_who, "new_joined_chat", subscriber=self.name)
        if rc:
            self.subscribers.append(with_who)
            self.console.write("You have contact with %s" % (with_who))

    def incoming_message(self, message):
        self.console.write(message)

    def run(self):
        while not self.done:
            try:
                self.dashi.consume(timeout=2)
            except socket.timeout, ex:
                pass

    def end(self):
        self.done = True
        self.input_message("%s has left the room" % (self.name))


def main(argv):
    global g_rabbit_url
    g_rabbit_url = argv[0]
    my_name = argv[1]
    console = TalkConsole()
    talker = DashiTalker(console, my_name)
    if len(argv) > 2:
        print "request"
        print argv[2]
        talker.request_conversation(argv[2])

    talker.start()
    done = False
    while not done:
        line = console.input()
        if line == "quit":
            done = True
            talker.end()
        else:
            talker.input_message(line)

if __name__ == '__main__':
    rc = main(sys.argv[1:])
    sys.exit(rc)
