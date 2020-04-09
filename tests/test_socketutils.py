import unittest

from dagger.netutils import *


class TestSocketUtils(unittest.TestCase):
    def test_resolve_uri(self):
        uri = "tcp://root:root@localhost:8080/hello/world?a=1&b="

        result = resolve_uri(uri)
        self.assertEqual(result.scheme, "tcp")
        self.assertEqual(result.query, "a=1&b=")
        self.assertEqual(result.username, "root")
        self.assertEqual(result.password, "root")
        self.assertEqual(result.host, "localhost")
        self.assertEqual(result.port, 8080)
        self.assertEqual(result.path, "/hello/world")

    def test_resolve_query(self):
        query = "a=1&b=2&c="
        ns = resolve_query(query)

        self.assertEqual(ns.a, "1")
        self.assertEqual(ns.b, "2")
        self.assertEqual(ns.c, "")
