import asyncio
import time
from typing import Optional, Dict

from dagger.configuration import (
    make_property,
    instance_checker,
    ConfigBase,
    int_format,
    float_format,
)
from dagger.declare import Declare
from dagger.logger import logger

__all__ = ("ServerConfiguration",)


class ServerConfiguration(ConfigBase):
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
    host: str = make_property("host", doc="server listen host", default="0.0.0.0")

    backlog: int = make_property(
        "backlog", formatter=int_format(min=1), doc="listen backlog", default=50
    )

    concurrency_limit = make_property(
        "concurrency_limit",
        doc="concurrency limit for one connection",
        formatter=int_format(min=1),
        default=5,
    )

    max_idle_time: int = make_property(
        "max_idle_time",
        doc="after this time flowed, idled connection would be closed",
        formatter=int_format(min=0),
        default=60,
    )

    server_name = make_property("server_name", doc="name of server", default=None)

    process = make_property(
        "process", formatter=int_format(min=1), doc="process number", default=1
    )

    worker_memory_limit = make_property(
        "worker_memory_limit", formatter=int_format(min=0), doc="reload after worker memory touch roof", default=0
    )

    log_level = make_property("log_level", doc="logging level", default="INFO")

    def __init__(self):
        self._declares: Dict[str, Declare] = {}
        self._server_state = None

    def register_declares(self, *declares: Declare):
        for declare in declares:
            assert isinstance(declare, Declare)
            self._declares[declare.name] = declare

    def get_declare(self, name: str):
        if name in self._declares:
            return self._declares[name]
        return Declare.make_dummy(name)

    def make_server(self):
        from dagger.server._server import Server

        return Server(self)

    @property
    def server_state(self) -> "ServerState":
        if self._server_state is None:
            self._server_state = ServerState(self)
        return self._server_state

    def __del__(self):
        self._server_state = None
        self._declares = None
        for i in dir(self):
            v = getattr(self, i)
            if isinstance(v, property):
                try:
                    object.__delattr__(self, i)
                except AttributeError:
                    pass


class ServerState:
    def __init__(self, configuration: ServerConfiguration):
        self._connections = {}
        self.configuration = configuration
        self._handle: Optional[asyncio.Handle] = None

    @property
    def connections(self):
        return self._connections

    def connection_lost(self, conn):
        self._connections.pop(conn, None)

    def connection_made(self, conn):
        self._connections[conn] = time.time()

    def connection_active(self, conn):
        self._connections[conn] = time.time()

    def _check_inactive(self):
        logger.debug("check inactive transport track-size=%d", len(self._connections))
        max_inactive_time = self.configuration.max_idle_time

        active = {}
        pending_close = []
        now = time.time()
        for transport, last_active_time in self._connections.items():
            if transport.is_closing():
                continue
            if now - last_active_time > max_inactive_time:
                pending_close.append(transport)
            else:
                active[transport] = last_active_time

        for v in pending_close:
            logger.info(
                "transport %s inactivate after %d seconds, close it now", v, max_inactive_time
            )
            v.close()

        self._connections = active
        self._schedule()

    def _schedule(self):
        configuration = self.configuration
        if self._handle:
            self._handle.cancel()

        self._handle = configuration.loop.call_later(
            configuration.max_idle_time, self._check_inactive
        )

    def start_monitor(self):
        configuration = self.configuration
        logger.info(
            "start remove idle(%s s) transport schedule every %d seconds",
            configuration.max_idle_time,
            configuration.max_idle_time,
        )
        self._schedule()

    def current_size(self):
        return len(self._connections)
