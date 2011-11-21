import unittest
import threading
from functools import partial
import itertools
import uuid

import dashi

import dashi.util

from dashi.tests.util import who_is_calling

log = dashi.util.get_logger()

_NO_REPLY = object()

class TestReceiver(object):
    def __init__(self, **kwargs):

        if 'name' in kwargs:
            self.name = kwargs['name']
        else:
            self.name = who_is_calling() + "." + uuid.uuid4().hex
        kwargs['name'] = self.name

        self.conn = dashi.DashiConnection(**kwargs)
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
        self.conn.consume(count=count, timeout=5)

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
        receiver = TestReceiver(uri=self.uri, exchange="x1")
        receiver.handle("test")
        receiver.handle("test2")

        conn = dashi.DashiConnection("s1", self.uri, "x1")
        args1 = dict(a=1, b="sandwich")
        conn.fire(receiver.name, "test", **args1)

        receiver.consume_n(1)

        self.assertEqual(len(receiver.received), 1)
        opname, gotargs = receiver.received[0]
        self.assertEqual(opname, "test")
        self.assertEqual(gotargs, args1)

        args2 = dict(a=2, b="burrito")
        args3 = dict(a=3)

        conn.fire(receiver.name, "test", **args2)
        conn.fire(receiver.name, "test2", **args3)

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
        receiver = TestReceiver(uri=self.uri, exchange="x1")
        replies = [5,4,3,2,1]
        receiver.handle("test", replies.pop)
        receiver.consume_n_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1")
        args1 = dict(a=1, b="sandwich")

        ret = conn.call(receiver.name, "test", **args1)
        self.assertEqual(ret, 1)
        receiver.join_consumer_thread()

        receiver.consume_n_in_thread(4)

        for i in list(reversed(replies)):
            ret = conn.call(receiver.name, "test", **args1)
            self.assertEqual(ret, i)
            
        receiver.join_consumer_thread()

    def test_call_unknown_op(self):
        receiver = TestReceiver(uri=self.uri, exchange="x1")
        receiver.handle("test", True)
        receiver.consume_n_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1")

        try:
            conn.call(receiver.name, "notarealop")
        except dashi.UnknownOperationError:
            pass
        else:
            self.fail("Expected UnknownOperationError")
        finally:
            receiver.join_consumer_thread()

    def test_call_handler_error(self):
        def raise_hell():
            raise Exception("hell")

        receiver = TestReceiver(uri=self.uri, exchange="x1")
        receiver.handle("raiser", raise_hell)
        receiver.consume_n_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1")

        try:
            conn.call(receiver.name, "raiser")

        except dashi.DashiError:
            pass
        else:
            self.fail("Expected DashiError")
        finally:
            receiver.join_consumer_thread()

    def test_fire_many_receivers(self):
        extras = {}
        receivers = []
        receiver_name = None

        for i in range(10):
            receiver = TestReceiver(uri=self.uri, exchange="x1", **extras)
            if not receiver_name:
                receiver_name = receiver.name
                extras['name'] = receiver.name
            receiver.handle("test")
            receiver.consume_n_in_thread(5)

            receivers.append(receiver)

        conn = dashi.DashiConnection("s1", self.uri, "x1")
        for i in range(50):
            conn.fire(receiver_name, "test", n=i)

        map = list(itertools.repeat(None, 50))
        for index, receiver in enumerate(receivers):
            receiver.join_consumer_thread()

            # each receiver should have gotten 5 messages, but they won't all
            # be consecutive

            self.assertEqual(len(receiver.received), 5)
            for opname,args in receiver.received:
                n = args['n']
                self.assertEqual(map[n], None)
                map[n] = index

        for msg_index, receiver_index in enumerate(map):
            self.assertIsNotNone(receiver_index)

        self.assertNotEquals(map, sorted(map))


class RabbitDashiConnectionTests(DashiConnectionTests):
    uri = "amqp://guest:guest@127.0.0.1//"