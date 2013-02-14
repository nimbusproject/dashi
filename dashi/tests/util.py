import sys
import subprocess
import errno
import os
import unittest
import signal
import socket


def who_is_calling():
    """Returns the name of the caller's calling function.

    Just a hacky way to pin things to test method names.
    There must be a better way.
    """
    return sys._getframe(2).f_code.co_name


class SocatProxy(object):
    """Manages a TCP forking proxy using socat
    """

    def __init__(self, destination, source_port=None):
        self.port = source_port or free_port()
        self.address = "localhost:%d" % self.port
        self.destination = destination
        self.process = None

    def start(self):
        assert not self.process
        src_arg = "TCP4-LISTEN:%d,fork,reuseaddr" % self.port
        dest_arg = "TCP4:%s" % self.destination
        try:
            self.process = subprocess.Popen(args=["socat", src_arg, dest_arg],
                preexec_fn=os.setpgrp)
        except OSError, e:
            if e.errno == errno.ENOENT:
                raise unittest.SkipTest("socat executable not found")

    def stop(self):
        if self.process and self.process.returncode is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except OSError, e:
                if e.errno != errno.ESRCH:
                    raise
            self.process.wait()
            self.process = None
            return True
        return False

    def restart(self):
        self.stop()
        self.start()

    @property
    def running(self):
        return self.process and self.process.returncode is None


def free_port(host="localhost"):
    """Pick a free port on a local interface and return it.

    Races are possible but unlikely
    """
    sock = socket.socket()
    try:
        sock.bind((host, 0))
        return sock.getsockname()[1]
    finally:
        sock.close()
