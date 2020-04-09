import unittest
import argparse

from dagger.configuration import *


class TestConfig(unittest.TestCase):
    def test_config(self):
        class Config(ConfigBase):
            max_connections = make_property(
                "max_conections",
                doc="max connections in pool",
                formatter=int_format(min=1),
                default=32,
            )

        config = Config()
        self.assertEqual(config.max_connections, 32)
        parser = argparse.ArgumentParser()

        config.register_to_argument_parser(parser)

        args = parser.parse_args(["--max-connections", "1"])

        config.set_from_namespace(args)
        self.assertEqual(config.max_connections, 1)
