import asyncio
from collections import Counter, deque
from functools import partial
from typing import Optional, Dict, Tuple, NamedTuple, Any, Deque

from dagger.client._syncpool import BasePool

from dagger.client._configuration import ClientConfiguration
from dagger.client._request import Request
from dagger.parser import Parser, ParserProtocol
from dagger.codec import decode_header, Header, EventType, unpack_payload
from dagger.exceptions import FrameError, get_exception_from_code
from dagger.logger import logger

__all__ = ("DefaultClientProtocol",)


class Message(NamedTuple):
    sequence_number: int
    body: Any
    error: int


class DefaultClientProtocol(asyncio.Protocol, ParserProtocol):
    __slots__ = (
        "_transport",
        "configuration",
        "_parser",
        "_waiters",
    )
    header_size = 8
    parser_class = Parser

    def __init__(self, configuration: ClientConfiguration):
        self._transport: Optional[asyncio.Transport] = None
        self.configuration = configuration

        self._parser = self.parser_class(self)

        self._waiters: Dict[int, asyncio.Future] = {}

    def closed(self):
        return self._transport.is_closing()

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self.configuration.loop

    # parser protocol

    def parse_header(self, data: bytes) -> (int, Header):
        header = decode_header(data)
        if header.event_type != EventType.RESPONSE:
            raise FrameError("expect %s, got %d" % (EventType.RESPONSE, header.event_type))

        return header.payload_size, header

    def parse_payload(self, header: Header, data: bytes):
        body = unpack_payload(header.compress_flag, data)

        if header.errno:
            exc = get_exception_from_code(header.errno)
            body = exc(body)

        return Message(header.sequence_number, body, header.errno)

    def on_message_complete(self, header: Header, message: Message):
        fut = self._waiters.get(message.sequence_number)
        if fut is None:
            return

        if message.error:
            fut.set_exception(message.body)
        else:
            fut.set_result(message.body)

        del self._waiters[message.sequence_number]

    # async protocol

    def connection_made(self, transport: asyncio.Transport) -> None:
        self._transport = transport

    def data_received(self, data: bytes) -> None:
        logger.debug("Connection recv data: %s, size=%d", self, len(data))
        self._parser.feed_data(data)

    def connection_lost(self, exc):
        if exc is None:
            exc = ConnectionError("connection lost")

        for fut in self._waiters.values():
            if not fut.done():
                fut.set_exception(exc)

        logger.debug("Connection lost: %s, exc=%r", self.getpeername(), exc)

    # request sender
    async def dispatch_request(self, request: Request):
        fut = self._send_request(request)
        return await fut

    def _send_request(self, request: Request) -> asyncio.Future:
        if request.sequence_number in self._waiters:
            raise RuntimeError("sequence id is duplicated")
        if self._transport.is_closing():
            raise ConnectionError("connection lost")

        payload = request.pack()
        self._transport.write(payload)
        fut = self.loop.create_future()
        self._waiters[request.sequence_number] = fut
        return fut

    def getpeername(self):
        if not self._transport:
            return
        return self._transport.get_extra_info("peername")

    def __str__(self):
        return f"<{self.__class__.__name__} transport={self._transport} peer={self.getpeername()}>"

    __repr__ = __str__

    def __getstate__(self):
        raise TypeError("Cannot serialize connection object")


class AsyncPool(BasePool):
    def __init__(self, configuration: ClientConfiguration, protocol_factory=None):
        super().__init__(configuration)
        self._counter = Counter()
        self._conns: Deque[DefaultClientProtocol] = deque(maxlen=configuration.pool_size)
        if protocol_factory:
            self._protocol_factory = protocol_factory
        else:
            self._protocol_factory = partial(DefaultClientProtocol, configuration)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._configuration.loop

    def is_busy(self):
        if not self._conns:
            return True
        if len(self._conns) == self._conns.maxlen:
            return False
        for conn, count in self._counter.items():
            if count > 8:
                return True
        return False

    async def _make_new_connection(self):
        host = self._configuration.host
        port = self._configuration.port
        loop = self.loop
        transport, protocol = await loop.create_connection(self._protocol_factory, host, port)
        return protocol

    async def dispatch_request(self, request: Request):
        # args should be checking in declare
        if self.is_busy():
            conn = await self._make_new_connection()
        else:
            conn = self._conns.popleft()
            if conn.closed():
                conn = await self._make_new_connection()

        self._conns.append(conn)
        fut = conn.dispatch_request(request)
        self._counter.update((conn,))
        try:
            rv = await fut
        except Exception:
            if conn.closed():
                self._conns.remove(conn)
                self._counter.pop(conn, None)
            raise
        else:
            self._counter.subtract((conn,))
            return rv
