import threading
import unittest

from dashi.util import LoopingCall

class LoopingCallTests(unittest.TestCase):

    def setUp(self):
        self.calls = 0
        self.passed = []

        self.loop = None

        # tests can set this to make looper stop itself after a specified
        # number of calls. self.loop must also be set.
        self.max_calls = None

        # when looper kills itself, it will set this event
        self.stopped = threading.Event()

    def assertPassed(self, index, *args, **kwargs):
        passed_args, passed_kwargs = self.passed[index]
        self.assertEqual(args, passed_args)
        self.assertEqual(kwargs, passed_kwargs)

    def assertLastPassed(self, *args, **kwargs):
        self.assertPassed(-1, *args, **kwargs)

    def looper(self, *args, **kwargs):
        self.calls += 1
        self.passed.append((args, kwargs))

        if self.max_calls and self.calls >= self.max_calls:
            self.loop.stop()
            self.stopped.set()

    def test_start_stop(self):
        loop = LoopingCall(self.looper, 1, hats=True)

        loop.start(1)
        loop.stop()
        self.assertEqual(self.calls, 1)
        self.assertLastPassed(1, hats=True)

    def test_start_stop_2(self):
        loop = LoopingCall(self.looper, 1, hats=True)

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

        self.assertFalse(loop.running)
        self.assertEqual(self.calls, 3)
        self.assertPassed(0, 1, 2, anarg=5)
        self.assertPassed(1, 1, 2, anarg=5)
        self.assertPassed(2, 1, 2, anarg=5)

        



