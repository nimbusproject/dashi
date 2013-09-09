#!/usr/bin/env python

from dashi.bootstrap import Service


class TestService(Service):

    def __init__(self, *args, **kwargs):
        super(TestService, self).__init__(*args, **kwargs)
        self.log = self.get_logger()

        self.log.info("%s started" % self.__class__.__name__)
        self.log.info("config: %s" % self.CFG)

if __name__ == "__main__":
    TestService()
