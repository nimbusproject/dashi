#!/usr/bin/env python

import uuid

import logging
logging.basicConfig(level=logging.DEBUG)

import gevent
import gevent.monkey
gevent.monkey.patch_all()

from dashi import DashiConnection


class HeartbeatSubscriber(object):

    def __init__(self):
        self.topic = "sub"+uuid.uuid4().hex

    def start(self):
        self.dashi = DashiConnection(self.topic, "amqp://guest:guest@127.0.0.1//",
                "heartbeater")
        self.dashi.handle(self.heartbeat)
        consumer = gevent.spawn(self.dashi.consume)

        # send subscribe request
        ret = self.dashi.call("heartbeater", "subscribe", subscriber=self.topic)
        assert ret is True

        consumer.join()

    def heartbeat(self, beat):
        print "Got heartbeat: %s" % beat

if __name__ == '__main__':
    HeartbeatSubscriber().start()
