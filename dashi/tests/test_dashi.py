import unittest
import threading
from functools import partial
import itertools
import uuid
import logging
import time
import sys

from kombu.pools import connections
import kombu.pools
from mock import patch

import dashi
import dashi.util
from dashi.tests.util import who_is_calling, SocatProxy, get_queue_info
from dashi.exceptions import NotFoundError

log = logging.getLogger(__name__)

_NO_EXCEPTION = object()
_NO_REPLY = object()

retry = dashi.util.RetryBackoff(max_attempts=10, backoff_max=3.0)


def assert_kombu_pools_empty():

    # hacky. peek into kombu internals to ensure pools
    # have no dirty resources.

    for pool in kombu.pools._all_pools():
        if pool._dirty:

            msg_parts = ["Pool %s has %d items:" % (pool, len(pool._dirty))]
            found_stack = False
            for c in pool._dirty:

                if hasattr(c, 'acquired_by') and c.acquired_by:
                    found_stack = True
                    stack = " Acquired by: " + "".join(c.acquired_by[-1])
                else:
                    stack = ""

                msg_parts.append("%s%s" % (c, stack))

            if not found_stack:
                msg_parts.append("\nPROTIP: export KOMBU_DEBUG_POOL and " +
                    "connection holder callstacks will be included in this error")

            raise Exception("\n".join(msg_parts))


class TestReceiver(object):

    consume_timeout = 300

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
        self.raise_exception = {}

        self.consumer_thread = None
        self.condition = threading.Condition()

    def handle(self, opname, reply_with=_NO_REPLY, raise_exception=_NO_EXCEPTION, **kwargs):
        if raise_exception is not _NO_EXCEPTION:
            self.raise_exception[opname] = raise_exception

        if reply_with is not _NO_REPLY:
            self.reply_with[opname] = reply_with
        self.conn.handle(partial(self._handler, opname), opname, **kwargs)

    def _handler(self, opname, **kwargs):
        log.debug("TestReceiver(%s) got op=%s: %s", self.name, opname, kwargs)
        with self.condition:
            self.received.append((opname, kwargs))
            self.condition.notifyAll()

        if opname in self.raise_exception:
            raise_exception = self.raise_exception[opname]
            raise raise_exception(opname)

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
                    raise Exception("timed out waiting for messages. had: %s", self.received)
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

    def disconnect(self):
        self.conn.disconnect()

    @property
    def queue(self):
        return self.conn._consumer._queue


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

        receiver.disconnect()
        assert_kombu_pools_empty()

    def test_call(self):
        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        replies = [5, 4, 3, 2, 1]
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
        receiver.disconnect()

        assert_kombu_pools_empty()

    def test_sysname(self):

        sysname = "sysname"

        receiver = TestReceiver(uri=self.uri, exchange="x1", sysname=sysname,
            transport_options=self.transport_options)
        replies = [5, 4, 3, 2, 1]
        receiver.handle("test", replies.pop)
        receiver.consume_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1", sysname=sysname,
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
        receiver.disconnect()

        assert_kombu_pools_empty()

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

        receiver.disconnect()
        assert_kombu_pools_empty()

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

        receiver.disconnect()
        assert_kombu_pools_empty()

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

        for receiver in receivers:
            receiver.disconnect()
        assert_kombu_pools_empty()

    def test_cancel(self):

        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("nothing", 1)
        receiver.consume_in_thread(1)

        receiver.cancel()

        # this should hang forever if cancel doesn't work
        receiver.join_consumer_thread()

        receiver.disconnect()
        assert_kombu_pools_empty()

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

        receiver.disconnect()
        assert_kombu_pools_empty()

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

        receiver.disconnect()
        assert_kombu_pools_empty()

    def test_exceptions(self):
        class CustomNotFoundError(Exception):
            pass

        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.conn.link_exceptions(custom_exception=CustomNotFoundError, dashi_exception=dashi.exceptions.NotFoundError)
        receiver.handle("test_exception", raise_exception=CustomNotFoundError, sender_kwarg="sender")
        receiver.consume_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)
        args1 = dict(a=1, b="sandwich")

        try:
            conn.call(receiver.name, "test_exception", **args1)
        except dashi.exceptions.NotFoundError:
            pass
        else:
            self.fail("Expected NotFoundError")
        finally:
            receiver.join_consumer_thread()

        receiver.disconnect()
        assert_kombu_pools_empty()

    def test_call_timeout(self):
        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)

        countdown = dashi.util.Countdown(0.6)
        # call with no receiver should timeout
        self.assertRaises(conn.timeout_error, conn.call, "notarealname", "test", timeout=0.5)
        delta = countdown.delta_seconds
        assert 0 < delta < 1, "delta: %s" % delta

    def test_call_queue_deleted(self):
        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options)
        receiver.handle("test", "hats")
        receiver.consume_in_thread(1)

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)
        args1 = dict(a=1, b="sandwich")

        rpc_consumers = []

        # patch in this fake consumer so we can save a copy
        class _Consumer(dashi.Consumer):
            def __init__(self, *args, **kwargs):
                super(_Consumer, self).__init__(*args, **kwargs)
                rpc_consumers.append(self)

        with patch.object(dashi, "Consumer", new=_Consumer):
            self.assertEqual(conn.call(receiver.name, "test", **args1), "hats")

        self.assertEqual(len(rpc_consumers), 1)
        self.assertEqual(len(rpc_consumers[0].queues), 1)
        queue = rpc_consumers[0].queues[0]

        self.assertRaises(NotFoundError, get_queue_info, conn, queue)

        receiver.join_consumer_thread()
        assert_kombu_pools_empty()


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

        receiver.disconnect()
        assert_kombu_pools_empty()

    def _thread_erroneous_replies(self, dashiconn, count):
        log.debug("sending erroneous replies")
        for i in range(count):
            try:
                dashiconn.reply(uuid.uuid4().hex, "alkjewfawlefja")
            except Exception:
                log.exception("Got expected exception replying to a nonexistent exchange")

    def test_pool_problems(self):

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

        receiver.cancel()
        receiver.join_consumer_thread()
        receiver.disconnect()


class RabbitProxyDashiConnectionTests(RabbitDashiConnectionTests):
    """Test rabbitmq dashi through a TCP proxy that we can kill to simulate failures

    Run all the above rabbit tests too, to make sure proxy behaves ok
    """

    @classmethod
    def setUpClass(cls):
        cls.proxy = SocatProxy("localhost:5672")
        cls.proxy.start()
        cls.uri = "amqp://guest:guest@localhost:%s" % cls.proxy.port
        cls.real_uri = "amqp://guest:guest@localhost:%s" % 5672

    @classmethod
    def tearDownClass(cls):
        if cls.proxy:
            cls.proxy.stop()

    def setUp(self):
        if not self.proxy.running:
            self.proxy.start()

    def _make_chained_proxy(self, proxy):
        second_proxy = SocatProxy(proxy.address, destination_options="ignoreeof")
        self.addCleanup(second_proxy.stop)
        second_proxy.start()

        uri = "amqp://guest:guest@localhost:%s" % second_proxy.port
        return second_proxy, uri

    def test_call_kill_pool_connection(self):
        # use a pool connection, kill the connection, and then try to reuse it

        # put receiver directly on rabbit. not via proxy
        receiver = TestReceiver(uri=self.real_uri, exchange="x1",
            transport_options=self.transport_options, retry=retry)
        replies = [5, 4, 3, 2, 1]
        receiver.handle("test", replies.pop)
        receiver.consume_in_thread()

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options, retry=retry)

        ret = conn.call(receiver.name, "test")
        self.assertEqual(ret, 1)

        for i in list(reversed(replies)):
            self.proxy.restart()
            ret = conn.call(receiver.name, "test")
            self.assertEqual(ret, i)

        receiver.cancel()
        receiver.join_consumer_thread()
        receiver.disconnect()

        assert_kombu_pools_empty()

    def test_call_kill_before_reply(self):

        # have the receiver handler restart the sender's connection
        # while it is waiting for a reply

        def killit():
            self.proxy.restart()
            return True

        # put receiver directly on rabbit. not via proxy
        receiver = TestReceiver(uri=self.real_uri, exchange="x1",
            transport_options=self.transport_options, retry=retry)
        receiver.handle("killme", killit)
        receiver.consume_in_thread()

        for _ in range(5):
            conn = dashi.DashiConnection("s1", self.uri, "x1",
                transport_options=self.transport_options, retry=retry)
            ret = conn.call(receiver.name, "killme")
            self.assertEqual(ret, True)

        receiver.cancel()
        receiver.join_consumer_thread()
        receiver.disconnect()

        assert_kombu_pools_empty()

    def test_fire_kill_pool_connection(self):
        # use a pool connection, kill the connection, and then try to reuse it

        # put receiver directly on rabbit. not via proxy
        receiver = TestReceiver(uri=self.real_uri, exchange="x1",
            transport_options=self.transport_options, retry=retry)
        receiver.handle("test")
        receiver.consume_in_thread()

        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options, retry=retry)

        conn.fire(receiver.name, "test", hats=0)
        receiver.wait(pred=lambda r: len(r) == 1)
        self.assertEqual(receiver.received[0], ("test", {"hats": 0}))

        for i in range(1, 4):
            self.proxy.restart()
            conn.fire(receiver.name, "test", hats=i)

        receiver.wait(pred=lambda r: len(r) == 4)
        self.assertEqual(receiver.received[1], ("test", {"hats": 1}))
        self.assertEqual(receiver.received[2], ("test", {"hats": 2}))
        self.assertEqual(receiver.received[3], ("test", {"hats": 3}))

        receiver.cancel()
        receiver.join_consumer_thread()
        receiver.disconnect()

        assert_kombu_pools_empty()

    def test_receiver_kill_connection(self):
        def errback():
            log.debug("Errback called", exc_info=True)

        # restart a consumer's connection. it should reconnect and keep consuming
        receiver = TestReceiver(uri=self.uri, exchange="x1",
            transport_options=self.transport_options, retry=retry,
            errback=errback)
        receiver.handle("test", "hats")
        receiver.consume_in_thread()

        # put caller directly on rabbit, not proxy
        conn = dashi.DashiConnection("s1", self.real_uri, "x1",
            transport_options=self.transport_options, retry=retry)
        self.assertEqual(conn.call(receiver.name, "test"), "hats")

        self.proxy.restart()

        log.debug("Queue consumer count: %s", get_queue_info(conn, receiver.queue)[2])

        self.assertEqual(conn.call(receiver.name, "test", timeout=60), "hats")
        self.assertEqual(conn.call(receiver.name, "test"), "hats")

        receiver.cancel()
        receiver.join_consumer_thread()
        receiver.disconnect()

        assert_kombu_pools_empty()

    def test_heartbeat_kill(self):
        # create a second tier proxy. then we can kill the backend proxy
        # and our connection remains "open"
        chained_proxy, chained_uri = self._make_chained_proxy(self.proxy)

        event = threading.Event()

        # attach an errback to the receiver that is called by dashi
        # with any connection failures
        def errback():
            log.debug("Errback called", exc_info=True)
            exc = sys.exc_info()[1]
            if "Too many heartbeats missed" in str(exc):
                log.debug("we got the beat!")
                event.set()

        receiver = TestReceiver(uri=chained_uri, exchange="x1",
            transport_options=self.transport_options, heartbeat=2.0,
            errback=errback, retry=retry)
        receiver.handle("test", "hats")
        receiver.consume_in_thread()

        # put caller directly on rabbit, not proxy
        conn = dashi.DashiConnection("s1", self.real_uri, "x1",
            transport_options=self.transport_options, retry=retry)
        self.assertEqual(conn.call(receiver.name, "test"), "hats")

        # kill the proxy and wait for the errback from amqp client
        self.proxy.stop()

        # try a few times to get the heartbeat loss error. depending on
        # timing, sometimes we just get a connectionloss error
        for _ in range(4):
            event.wait(5)
            if event.is_set():
                break
            else:
                self.proxy.start()
                time.sleep(2)  # give it time to reconnect
                self.proxy.stop()
        assert event.is_set()

        # restart and we should be back up and running
        self.proxy.start()
        self.assertEqual(conn.call(receiver.name, "test"), "hats")

        receiver.cancel()
        receiver.join_consumer_thread()
        receiver.disconnect()

        assert_kombu_pools_empty()

    def test_call_timeout_during_recovery(self):
        conn = dashi.DashiConnection("s1", self.uri, "x1",
            transport_options=self.transport_options)

        got_timeout = threading.Event()

        def doit():
            self.assertRaises(conn.timeout_error, conn.call, "notarealname", "test", timeout=3)
            got_timeout.set()

        countdown = dashi.util.Countdown(4)

        t = threading.Thread(target=doit)
        t.daemon = True
        t.start()
        time.sleep(0.5)

        try:
            got_timeout.wait(5)
        finally:
            t.join()
        delta = countdown.delta_seconds
        assert 0 < delta < 1, "delta: %s" % delta
