import threading
import unittest

from dashi.util import LoopingCall, RetryBackoff


class LoopingCallTests(unittest.TestCase):

    def setUp(self):
        self.calls = 0
        self.passed = []
        self.condition = threading.Condition()

        self.loop = None

        # tests can set this to make looper stop itself after a specified
        # number of calls. self.loop must also be set.
        self.max_calls = None

        # tests can set this to make looper raise an exception
        self.raise_this = None

        # when looper kills itself, it will set this event
        self.stopped = threading.Event()

    def tearDown(self):
        if self.loop:
            # peek into loop and make sure thread is joined
            self.loop.stop()
            thread = self.loop.thread
            if thread:
                thread.join()

    def assertPassed(self, index, *args, **kwargs):
        passed_args, passed_kwargs = self.passed[index]
        self.assertEqual(args, passed_args)
        self.assertEqual(kwargs, passed_kwargs)

    def assertLastPassed(self, *args, **kwargs):
        self.assertPassed(-1, *args, **kwargs)

    def looper(self, *args, **kwargs):
        with self.condition:
            self.calls += 1
            self.passed.append((args, kwargs))
            self.condition.notifyAll()

        if self.max_calls and self.calls >= self.max_calls:
            self.loop.stop()
            self.stopped.set()

        if self.raise_this:
            raise self.raise_this

    def test_start_stop(self):
        self.loop = loop = LoopingCall(self.looper, 1, hats=True)

        loop.start(1)
        loop.stop()

        with self.condition:
            if not self.calls:
                self.condition.wait(5)

        self.assertEqual(self.calls, 1)
        self.assertLastPassed(1, hats=True)

    def test_start_stop_2(self):
        self.loop = loop = LoopingCall(self.looper, 1, hats=True)

        loop.start(1, now=False)
        loop.stop()
        self.assertEqual(self.calls, 0)

    def test_called(self):
        # looper will stop itself after 3 calls
        self.max_calls = 3
        self.loop = loop = LoopingCall(self.looper, 1, 2, anarg=5)

        # interval of 0 makes it not block
        loop.start(0)
        self.assertTrue(self.stopped.wait(5))

        #peek into looping call and join on thread
        thread = loop.thread
        if thread:
            thread.join()

        self.assertFalse(loop.running)
        self.assertEqual(self.calls, 3)
        self.assertPassed(0, 1, 2, anarg=5)
        self.assertPassed(1, 1, 2, anarg=5)
        self.assertPassed(2, 1, 2, anarg=5)

    def test_error_caught(self):
        self.loop = LoopingCall(self.looper)
        self.raise_this = Exception("too many sandwiches")

        self.loop.start(0)

        with self.condition:
            while not self.calls >= 3:

                self.condition.wait()

        self.loop.stop()
        self.assertGreaterEqual(self.calls, 3)


class TestRetryBackoff(unittest.TestCase):

    def test_max_attempts(self):
        backoff = RetryBackoff(max_attempts=5)
        self.assertEqual(list(backoff), [0.5, 1.0, 1.5, 2.0, 2.5])

    def test_max_attempts_float(self):
        backoff = RetryBackoff(max_attempts=5.6)
        self.assertEqual(list(backoff), [0.5, 1.0, 1.5, 2.0, 2.5])

    def test_backoff_start(self):
        backoff = RetryBackoff(max_attempts=5, backoff_start=1.0)
        self.assertEqual(list(backoff), [1.0, 1.5, 2.0, 2.5, 3.0])

    def test_backoff_start_int(self):
        backoff = RetryBackoff(max_attempts=5, backoff_start=1)
        self.assertEqual(list(backoff), [1.0, 1.5, 2.0, 2.5, 3.0])

    def test_backoff_step(self):
        backoff = RetryBackoff(max_attempts=5, backoff_step=1.0)
        self.assertEqual(list(backoff), [0.5, 1.5, 2.5, 3.5, 4.5])

    def test_backoff_step_int(self):
        backoff = RetryBackoff(max_attempts=5, backoff_step=1)
        self.assertEqual(list(backoff), [0.5, 1.5, 2.5, 3.5, 4.5])

    def test_backoff_max(self):
        backoff = RetryBackoff(max_attempts=5, backoff_max=1.5)
        self.assertEqual(list(backoff), [0.5, 1.0, 1.5, 1.5, 1.5])

    def test_backoff_max_int(self):
        backoff = RetryBackoff(max_attempts=5, backoff_max=1)
        self.assertEqual(list(backoff), [0.5, 1.0, 1.0, 1.0, 1.0])

    def test_timeout(self):
        backoff = RetryBackoff(max_attempts=5, timeout=1)
        result = list(backoff)
        expected = [1.0, 1.5, 2.0, 2.5, 3.0]
        for x, y in zip(result, expected):
            self.assertAlmostEqual(x, y, places=4)
