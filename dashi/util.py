import logging
import threading
import time


class Countdown(object):
    _time_func = time.time

    def __init__(self, timeout, time_func=None):
        if time_func is not None:
            self._time_func = time_func
        self.timeout = timeout
        self.expires = self._time_func() + timeout

    @classmethod
    def from_value(cls, timeout):
        """Wraps a timeout value in a Countdown, unless it already is
        """
        if isinstance(timeout, cls):
            return timeout
        return cls(timeout)

    @property
    def expired(self):
        return self._time_func() >= self.expires

    @property
    def timeleft(self):
        """Number of seconds remaining before timeout
        """
        return max(0.0, self.expires - self._time_func())

    @property
    def delta_seconds(self):
        """Difference in seconds (can be negative)"""
        return self.expires - self._time_func()


class RetryBackoff(object):
    def __init__(self, max_attempts=0, backoff_start=0.5, backoff_step=0.5,
                 backoff_max=30, timeout=None):
        self._max_attempts = int(max_attempts)
        self._backoff_start = float(backoff_start)
        self._backoff_step = float(backoff_step)
        self._backoff_max = float(backoff_max)
        self._timeout = Countdown.from_value(timeout) if timeout else None

    def __iter__(self):
        retry = 1
        backoff = self._backoff_start

        while not self._max_attempts or retry <= self._max_attempts:

            if self._timeout:
                timeleft = self._timeout.timeleft
                if not timeleft:
                    return
                backoff = max(backoff, timeleft)

            yield backoff

            backoff = min(backoff + self._backoff_step, self._backoff_max)
            retry += 1


class LoopingCall(object):
    def __init__(self, fun, *args, **kwargs):
        assert callable(fun)
        self.fun = fun
        self.args = args
        self.kwargs = kwargs

        self.thread = None
        self.interval = None

        self.cancelled = threading.Event()

        self.running = False

    def __del__(self):
        self.stop()

    def start(self, interval, now=True):
        assert self.thread is None
        self.cancelled.clear()

        self.running = True
        self.thread = threading.Thread(target=self._looper,
                                       args=(interval, now))
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        if self.thread:
            self.cancelled.set()

    def __call__(self):
        try:
            self.fun(*self.args, **self.kwargs)
        except Exception:
            log = logging.getLogger(__name__)
            log.exception("Error in looping call")

    def _looper(self, interval, now):
        try:
            if now:
                self()
            while not self.cancelled.is_set():
                cancelled = self.cancelled.wait(interval)
                if not cancelled:
                    self()
        finally:
            self.cancelled.clear()
            self.thread = None
            self.running = False
