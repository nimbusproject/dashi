import logging
import threading


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
