import logging
import threading

def get_logger():
    return logging.getLogger('dashi')


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

    def start(self, interval, now=True):
        assert self.thread is None

        self.running = True
        self.thread = threading.Thread(target=self._looper,
                                       args=(interval, now))
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.cancelled.set()
        self.thread = None

    def __call__(self):
        try:
            self.fun(*self.args, **self.kwargs)
        except Exception:
            log = get_logger()
            log.exception("Error in looping call")

    def _looper(self, interval, now):
        if now:
            self()
        while not self.cancelled.is_set():
            cancelled = self.cancelled.wait(interval)
            if not cancelled:
                self()
        self.cancelled.clear()
        self.running = False
