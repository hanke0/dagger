import asyncio
import socket
import signal
import os
from functools import partial
from typing import Optional

from dagger.netutils import create_default_listener
from dagger.logger import logger
from dagger.server._protocol import DefaultServerProtocol
from dagger.server._configuration import ServerConfiguration

__all__ = ("Server",)


def _create_non_block_listener(host: str, port: int) -> socket.socket:
    return create_default_listener(host, port, timeout=0, blocking=False)


class Server:
    def __init__(self, configuration: ServerConfiguration, protocol_factory=None):
        self._configuration = configuration
        self._listener: Optional[socket.socket] = None
        self._server: Optional[asyncio.AbstractServer] = None
        if protocol_factory:
            self._protocol_factory = protocol_factory
        else:
            self._protocol_factory = partial(DefaultServerProtocol, configuration)

    @property
    def loop(self):
        return self._configuration.loop

    def server_forever(self):
        configuration = self._configuration
        host = configuration.host
        port = configuration.port
        self._listener = _create_non_block_listener(host, port)
        self._listener.set_inheritable(True)
        server_coro = self.loop.create_server(
            self._protocol_factory, sock=self._listener, backlog=configuration.backlog
        )
        server = self.loop.run_until_complete(server_coro)
        self._server = server
        loop = self.loop
        loop.add_signal_handler(signal.SIGINT, loop.stop)
        loop.add_signal_handler(signal.SIGTERM, loop.stop)
        try:
            logger.info("Starting worker [%d]", os.getpid())

            if configuration.max_idle_time != 0:
                configuration.server_state.start_monitor()

            loop.run_forever()
        finally:
            logger.info("Stopping worker [%d]", os.getpid())
            server.close()
            # close server socket
            loop.run_until_complete(server.wait_closed())

            # close all connections
            connections = configuration.server_state.connections
            coros = []
            for connection in connections:
                connection.graceful_close()
                coros.append(connection.wait_closed())
            if coros:
                logger.info("Wait %d connection graceful close. [%d]", len(coros), os.getpid())
                loop.run_until_complete(asyncio.gather(*coros))

            loop.close()
            logger.info("Exit worker [%d]", os.getpid())
