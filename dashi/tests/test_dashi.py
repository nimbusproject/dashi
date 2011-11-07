import unittest

import dashi

class DashiConnectionTests(unittest.TestCase):

    uri = 'memory://hello'

    def test_fire(self):
        conn = dashi.DashiConnection("s1", self.uri, "alskdjf")

        conn.fire("s2", "test")

  