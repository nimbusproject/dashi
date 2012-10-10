import unittest
import threading
from functools import partial
import itertools
import uuid
import logging
import time

from nose.plugins.skip import SkipTest
from kombu.pools import connections

import dashi
import dashi.util
from dashi.tests.util import who_is_calling

log = logging.getLogger(__name__)

_NO_REPLY = object()

class TestReceiver(object):

    consume_timeout = 5

    def __init__(self, **kwargs):

        if 'name' in kwargs:
            self.name = kwargs['name']
        else:
            self.name = who_is_calling() + "." + uuid.uuid4().hex
        kwargs['name'] = self.name

        self.conn = dashi.DashiConnection(**kwargs)
        self.conn.consumer_timeout = 0.01
        self.received = []
        self.reply_with = {}

        self.consumer_thread = None
        self.condition = threading.Condition()

    def handle(self, opname, reply_with=_NO_REPLY, **kwargs):
        if reply_with is not _NO_REPLY:
            self.reply_with[opname] = reply_with
        self.conn.handle(partial(self._handler, opname), opname, **kwargs)

    def _handler(self, opname, **kwargs):
        with self.condition:
            self.received.append((opname, kwargs))
            self.condition.notifyAll()

        if opname in self.reply_with:
            reply_with = self.reply_with[opname]
            if callable(reply_with):
                return reply_with()
            return reply_with

    def wait(self, timeout=5, pred=None):

        if not pred:
            pred = lambda received: bool(received)

        start = time.time()
        remaining = timeout
        with self.condition:
            while not pred(self.received):
                self.condition.wait(remaining)
                now = time.time()
                if now - start >= timeout and not pred(self.received):
                    raise Exception("timed out waiting for messages")
                remaining -= now - start

    def consume(self, count):
        self.conn.consume(count=count, timeout=self.consume_timeout)

    def consume_in_thread(self, count=None):
        assert self.consumer_thread is None
        t = threading.Thread(target=self.consume, args=(count,))
        t.daemon = True
        self.consumer_thread = t
        t.start()

    def join_consumer_thread(self, cancel=False):
        if self.consumer_thread:
            if cancel:
                self.conn.cancel()
            self.consumer_thread.join()
            self.consumer_thread = None

    def clear(self):
        self.received[:] = []

    def cancel(self):
        self.conn.cancel()


class DashiConnectionTests(unittest.TestCase):

    uri = 'memory://hello'
    transport_options = dict(polling_interval=0.01)

    def test_fire(self):
        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("test")
        receiver.handle("test2")

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)
        args1 = dict(a=1, b="sandwich")
        conn.fire(receiver.name, "test", **args1)

        receiver.consume(1)

        self.assertEqual(len(receiver.received), 1)
        opname, gotargs = receiver.received[0]
        self.assertEqual(opname, "test")
        self.assertEqual(gotargs, args1)

        args2 = dict(a=2, b="burrito")
        args3 = dict(a=3)

        conn.fire(receiver.name, "test", **args2)
        conn.fire(receiver.name, "test2", **args3)

        receiver.clear()
        receiver.consume(2)

        self.assertEqual(len(receiver.received), 2)
        opname, gotargs = receiver.received[0]
        self.assertEqual(opname, "test")
        self.assertEqual(gotargs, args2)
        opname, gotargs = receiver.received[1]
        self.assertEqual(opname, "test2")
        self.assertEqual(gotargs, args3)

    def test_call(self):
        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        replies = [5,4,3,2,1]
        receiver.handle("test", replies.pop)
        receiver.consume_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)
        args1 = dict(a=1, b="sandwich")

        ret = conn.call(receiver.name, "test", **args1)
        self.assertEqual(ret, 1)
        receiver.join_consumer_thread()

        receiver.consume_in_thread(4)

        for i in list(reversed(replies)):
            ret = conn.call(receiver.name, "test", **args1)
            self.assertEqual(ret, i)

        receiver.join_consumer_thread()

    def test_call_unknown_op(self):
        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("test", True)
        receiver.consume_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)

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

        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("raiser", raise_hell)
        receiver.consume_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)

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

        for i in range(3):
            receiver = TestReceiver(uri=self.uri, exchange="x1",
                       transport_options=self.transport_options, **extras)
            if not receiver_name:
                receiver_name = receiver.name
                extras['name'] = receiver.name
            receiver.handle("test")
            receivers.append(receiver)

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)
        for i in range(10):
            conn.fire(receiver_name, "test", n=i)

        # walk the receivers and have each one consume a single message
        receiver_cycle = itertools.cycle(receivers)
        for i in range(10):
            receiver = next(receiver_cycle)
            receiver.consume(1)
            opname, args = receiver.received[-1]
            self.assertEqual(opname, "test")
            self.assertEqual(args['n'], i)

    def test_cancel(self):

        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("nothing", 1)
        receiver.consume_in_thread(1)

        receiver.cancel()

        # this should hang forever if cancel doesn't work
        receiver.join_consumer_thread()

    def test_cancel_resume_cancel(self):
        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("test", 1)
        receiver.consume_in_thread()

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)
        self.assertEqual(1, conn.call(receiver.name, "test"))

        receiver.cancel()
        receiver.join_consumer_thread()
        receiver.clear()

        # send message while receiver is cancelled
        conn.fire(receiver.name, "test", hats=4)

        # start up consumer again. message should arrive.
        receiver.consume_in_thread()

        receiver.wait()
        self.assertEqual(receiver.received[-1], ("test", dict(hats=4)))

        receiver.cancel()
        receiver.join_consumer_thread()

    def test_handle_sender_kwarg(self):
        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("test1", "hello", sender_kwarg="sender")
        receiver.handle("test2", "hi", sender_kwarg="spender")
        receiver.consume_in_thread()

        sender_name = uuid.uuid4().hex
        conn = dashi.DashiConnection(sender_name, self.uri, "x1",
            transport_options=self.transport_options)
        args = dict(a=1, b="sandwich")

        expected_args1 = args.copy()
        expected_args1['sender'] = sender_name

        expected_args2 = args.copy()
        expected_args2['spender'] = sender_name

        # first one is a fire
        conn.fire(receiver.name, "test1", args=args)
        receiver.wait()
        opname, gotargs = receiver.received[0]

        self.assertEqual(opname, "test1")
        self.assertEqual(gotargs, expected_args1)

        receiver.clear()

        # next try a call
        reply = conn.call(receiver.name, "test2", **args)
        self.assertEqual(reply, "hi")

        opname, gotargs = receiver.received[0]
        self.assertEqual(opname, "test2")
        self.assertEqual(gotargs, expected_args2)

        receiver.cancel()
        receiver.join_consumer_thread()


class RabbitDashiConnectionTests(DashiConnectionTests):
    """The base dashi tests run on rabbit, plus some extras which are
    rabbit specific
    """
    uri = "amqp://guest:guest@127.0.0.1//"

    def test_call_channel_free(self):

        # hackily ensure that call() releases its channel

        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("test", "myreply")
        receiver.consume_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)

        # peek into connection to grab a channel and note its id
        with connections[conn._conn].acquire(block=True) as kombuconn:
            with kombuconn.channel() as channel:
                channel_id = channel.channel_id
                log.debug("got channel ID %s", channel.channel_id)

        ret = conn.call(receiver.name, "test")
        self.assertEqual(ret, "myreply")
        receiver.join_consumer_thread()

        # peek into connection to grab a channel and note its id
        with connections[conn._conn].acquire(block=True) as kombuconn:
            with kombuconn.channel() as channel:
                log.debug("got channel ID %s", channel.channel_id)
                self.assertEqual(channel_id, channel.channel_id)

    def _thread_erroneous_replies(self, dashiconn, count):
        log.debug("sending erroneous replies")
        for i in range(count):
            try:
                dashiconn.reply(uuid.uuid4().hex, "alkjewfawlefja")
            except Exception:
                log.exception("Got expected exception replying to a nonexistent exchange")

    def test_pool_problems(self):
        raise SkipTest("failing test that exposes problem in dashi RPC strategy")

        # this test fails (I think) because replies are sent to a nonexistent
        # exchange. Rabbit freaks out about this and poisons the channel.
        # Eventually the sender thread comes across the poisoned channel and
        # its send fails. How to fix??

        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("test1")
        receiver.consume_in_thread()

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)

        t = threading.Thread(target=self._thread_erroneous_replies,
            args=(conn, 100))
        t.daemon = True
        t.start()

        try:
            for i in range(100):
                conn.fire(receiver.name, "test1", i=i)
        finally:
            t.join()

        pred = lambda received: len(received) == 100
        receiver.wait(pred=pred)

        self.assertEqual(len(receiver.received), 100)

