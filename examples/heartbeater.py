#!/usr/bin/env python

from itertools import count

import gevent
import gevent.monkey
gevent.monkey.patch_all()

import logging
logging.basicConfig(level=logging.DEBUG)

from dashi import DashiConnection


class HeartbeatService(object):

    def __init__(self):
        self.topic = "heartbeater"

        self.subscribers = []
        self.counter = count(0)

    def start(self):
        self.dashi = DashiConnection(self.topic, "amqp://guest:guest@127.0.0.1//",
                "heartbeater")

        # support the "subscribe" operation with this method
        self.dashi.handle(self.subscribe)

        gevent.spawn(self.dashi.consume)
        gevent.spawn(self._beater).join()

    def subscribe(self, subscriber):
        print "Got subscription: subscriber=%s" % subscriber
        self.subscribers.append(subscriber)
        return True

    def _beater(self):
        while True:
            n = self.counter.next()
            logging.debug("Beat %s", n)
            for subscriber in self.subscribers:

                self.dashi.fire(subscriber, "heartbeat", beat=n)

            gevent.sleep(1)

if __name__ == '__main__':
    HeartbeatService().start()
