import asyncio
import importlib
from typing import Set

from dagger.configuration import make_property, instance_checker, ConfigBase, int_format
from dagger.declare import Declare

__all__ = ("ClientConfiguration",)


class ClientConfiguration(ConfigBase):
    loop: asyncio.AbstractEventLoop = make_property(
        "loop",
        doc="event loop",
        formatter=instance_checker(asyncio.AbstractEventLoop),
        default=lambda: asyncio.get_event_loop(),
        configurable=False,
    )
    port: int = make_property(
        "port", doc="server listen port", formatter=int_format(max=65535, min=0), default=10050
    )
    host: str = make_property("host", doc="remote host", default="127.0.0.1")

    log_level = make_property("log_level", doc="logging level", default="INFO")

    pool_size = make_property(
        "pool_size", formatter=int_format(min=1), doc="max pool size", default=12
    )

    asynchronous = make_property(
        "asynchronous",
        doc="use asynchronous mode",
        default=False,
        parser_options={"action": "store_true"},
    )

    timeout = make_property("timeout", doc="timeout", default=300, formatter=int_format(min=0))

    max_retry = make_property(
        "max_retry",
        doc="retry this times when connection error",
        default=3,
        formatter=int_format(min=0),
    )

    def __init__(self):
        self.declares: Set[Declare] = set()

    def register_declares(self, *declares: Declare):
        for declare in declares:
            assert isinstance(declare, Declare)
            self.declares.add(declare)

    def register_declares_from_module(self, module):
        if isinstance(module, str):
            module = importlib.import_module(module)
        declares = self.declares
        for varname in dir(module):
            varval = getattr(module, varname)
            if isinstance(varval, Declare):
                declares.add(varval)

    def __del__(self):
        self.declares = None
        for i in dir(self):
            v = getattr(self, i)
            if isinstance(v, property):
                try:
                    object.__delattr__(self, i)
                except AttributeError:
                    pass
