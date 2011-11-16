import unittest
import threading
from functools import partial

import dashi

_NO_REPLY = object()

class TestReceiver(object):
    def __init__(self, *args, **kwargs):
        self.conn = dashi.DashiConnection(*args, **kwargs)
        self.received = []
        self.reply_with = {}

        self.consumer_thread = None

    def handle(self, opname, reply_with=_NO_REPLY):
        if reply_with is not _NO_REPLY:
            self.reply_with[opname] = reply_with
        self.conn.handle(partial(self._handler, opname), opname)

    def _handler(self, opname, **kwargs):
        self.received.append((opname, kwargs))
        if opname in self.reply_with:
            reply_with = self.reply_with[opname]
            if callable(reply_with):
                return reply_with()
            return reply_with

    def consume_n(self, count):
        self.conn.consume(count=count)

    def consume_n_in_thread(self, count):
        assert self.consumer_thread is None
        t = threading.Thread(target=self.consume_n, args=(count,))
        t.daemon = True
        self.consumer_thread = t
        t.start()

    def join_consumer_thread(self):
        if self.consumer_thread:
            self.consumer_thread.join()
            self.consumer_thread = None

    def clear(self):
        self.received[:] = []


class DashiConnectionTests(unittest.TestCase):

    uri = 'memory://hello'

    def test_fire(self):
        receiver = TestReceiver("r1", self.uri, "x1")
        receiver.handle("test")
        receiver.handle("test2")

        conn = dashi.DashiConnection("s1", self.uri, "x1")
        args1 = dict(a=1, b="sandwich")
        conn.fire("r1", "test", **args1)

        receiver.consume_n(1)

        self.assertEqual(len(receiver.received), 1)
        opname, gotargs = receiver.received[0]
        self.assertEqual(opname, "test")
        self.assertEqual(gotargs, args1)

        args2 = dict(a=2, b="burrito")
        args3 = dict(a=3)

        conn.fire("r1", "test", **args2)
        conn.fire("r1", "test2", **args3)

        receiver.clear()
        receiver.consume_n(2)

        self.assertEqual(len(receiver.received), 2)
        opname, gotargs = receiver.received[0]
        self.assertEqual(opname, "test")
        self.assertEqual(gotargs, args2)
        opname, gotargs = receiver.received[1]
        self.assertEqual(opname, "test2")
        self.assertEqual(gotargs, args3)

    def test_call(self):
        receiver = TestReceiver("r1", self.uri, "x1")
        replies = [5,4,3,2,1]
        receiver.handle("test", replies.pop)
        receiver.consume_n_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1")
        args1 = dict(a=1, b="sandwich")

        ret = conn.call("r1", "test", **args1)
        self.assertEqual(ret, 1)
        receiver.join_consumer_thread()

        receiver.consume_n_in_thread(4)

        for i in list(reversed(replies)):
            ret = conn.call("r1", "test", **args1)
            self.assertEqual(ret, i)
            
        receiver.join_consumer_thread()
