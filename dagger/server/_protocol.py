import asyncio
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from typing import Optional, Set, Deque

from dagger.exceptions import FrameError, PackUnpackError, ContentVerifyFailed
from dagger.codec import pack_message, EventType, unpack_payload, decode_header, Header
from dagger.parser import ParserProtocol, Parser
from dagger.logger import logger
from dagger.server._configuration import ServerConfiguration

__all__ = ("DefaultServerProtocol",)

_ManagerPool = ThreadPoolExecutor(32, "ManagerThread-")


class Message:
    __slots__ = ("sequence_number", "method", "args")

    def __init__(self, sequence_number: int, method: str, args: list):
        self.sequence_number = sequence_number
        self.method = method
        self.args = args


class FlowControl:
    __slots__ = ("_transport", "read_paused", "write_paused", "_is_writable_event")

    def __init__(self, transport):
        self._transport = transport
        self.read_paused = False
        self.write_paused = False
        self._is_writable_event = asyncio.Event()
        self._is_writable_event.set()

    async def drain(self):
        await self._is_writable_event.wait()

    def pause_reading(self):
        if not self.read_paused:
            self.read_paused = True
            self._transport.pause_reading()

    def resume_reading(self):
        if self.read_paused:
            self.read_paused = False
            self._transport.resume_reading()

    def pause_writing(self):
        if not self.write_paused:
            self.write_paused = True
            self._is_writable_event.clear()

    def resume_writing(self):
        if self.write_paused:
            self.write_paused = False
            self._is_writable_event.set()


class DefaultServerProtocol(asyncio.Protocol, ParserProtocol):
    __slots__ = (
        "_transport",
        "configuration",
        "_parser",
        "_pending_message",
        "_running_tasks",
        "should_close",
        "count",
        "flow",
    )
    parser_class = Parser
    header_size = 8

    def __init__(self, configuration: ServerConfiguration):
        self.configuration = configuration

        self._parser = self.parser_class(self)
        self._pending_message: Deque[Message] = deque()
        self._running_tasks: Set[asyncio.Task] = set()

        self.should_close = None
        self._transport: Optional[asyncio.Transport] = None
        self.flow: Optional[FlowControl] = None

        self.count = 0

    # parser protocol

    def parse_header(self, data: bytes) -> (int, Header):
        header = decode_header(data)
        if header.event_type != EventType.REQUEST:
            raise FrameError(f"Invalid event type: {header.event_type}")

        return header.payload_size, header

    def parse_payload(self, header: Header, data: bytes):
        body = unpack_payload(header.compress_flag, data)
        if not isinstance(body, list) or len(body) != 2:
            raise ContentVerifyFailed(f"invalid request: {body}")

        method, args = body
        if not isinstance(args, list) or not isinstance(method, str):
            raise ContentVerifyFailed(f"invalid request: {body}")
        return Message(header.sequence_number, method, args)

    def on_message_complete(self, header: Header, message: Message):
        concurrency_limit = self.configuration.concurrency_limit
        running_events = self._running_tasks
        if concurrency_limit != 0 and len(running_events) > concurrency_limit:
            logger.debug(self, "offend flow control")
            self._pending_message.append(message)
            self.flow.pause_reading()
        else:
            self._consume_one_message(message)

    # async Protocol

    def connection_made(self, transport: asyncio.Transport) -> None:
        self._transport = transport
        self.flow = FlowControl(transport)
        self.configuration.server_state.connection_made(self)
        logger.info(
            "Connection made: %s, monitored=%d",
            self.getpeername(),
            self.configuration.server_state.current_size(),
        )

    def data_received(self, data: bytes) -> None:
        self.configuration.server_state.connection_active(self)
        if logger.isEnabledFor(10):  # debug log level
            logger.debug("Connection recv data: %s, size=%d", self, len(data))
        try:
            self._parser.feed_data(data)
        except Exception as exc:
            self._fatal(exc)

    def connection_lost(self, exc):
        self.configuration.server_state.connection_lost(self)
        logger.info(
            "Connection lost: %s, consumed-event=%d, monitored=%d exc=%r",
            self.getpeername(),
            self.count,
            self.configuration.server_state.current_size(),
            exc,
        )

    def pause_writing(self):
        """Called by the transport when the write buffer exceeds the high water mark."""
        self.flow.pause_writing()

    def resume_writing(self):
        """Called by the transport when the write buffer drops below the low water mark."""
        self.flow.resume_writing()

    # message about inner method

    def _consume_one_message(self, msg: Message):
        task = self.loop.create_task(self._response_handler(msg))
        if not task.done():
            self._running_tasks.add(task)
            task.add_done_callback(self._message_done_cb)
        self.count += 1

    def _message_done_cb(self, task):
        self._running_tasks.discard(task)
        if self._transport.is_closing():
            return

        concurrency_limit = self.configuration.concurrency_limit
        if concurrency_limit == 0:
            return

        running = self._running_tasks
        pending = self._pending_message
        if pending:
            while len(running) < concurrency_limit and pending:
                msg = pending.popleft()
                self._consume_one_message(msg)

        self.flow.resume_reading()

    async def _response_handler(self, msg: Message):
        transport = self._transport
        configuration = self.configuration
        peername = self.getpeername()
        loop = self.loop

        logger.debug("Connection %s prepare consume request seq=%d", peername, msg.sequence_number)
        declare = configuration.get_declare(msg.method)
        args = msg.args
        if declare.runmode == declare.SYNC_RUN:
            try:
                rv = declare.server_call(*args)
            except Exception as e:
                rv = e

        elif declare.runmode == declare.ASYNC_RUN:
            try:
                rv = await declare.server_call(*args)
            except Exception as e:
                rv = e
        else:
            try:
                rv = await loop.run_in_executor(_ManagerPool, declare.server_call, *args)
            except Exception as e:
                rv = e
        try:
            payload = pack_message(msg.sequence_number, EventType.RESPONSE.value, rv)
        except PackUnpackError as exc:
            payload = pack_message(msg.sequence_number, EventType.RESPONSE.value, exc)

        if not self._transport.is_closing():
            if self.flow.write_paused:
                await self.flow.drain()
            transport.write(payload)
            self.configuration.server_state.connection_active(self)
            self.flow.resume_reading()

        if isinstance(rv, Exception):
            logger.exception(
                "Connection %s raise error when consume seq=%d",
                peername,
                msg.sequence_number,
                exc_info=rv,
            )
        else:
            logger.debug(
                "Connection %s finish write consume request %d", peername, msg.sequence_number
            )
        return

    # close handler

    def _fatal(self, exc):
        self.should_close = True
        self.flow.pause_reading()
        self._transport.close()
        logger.error("transport %s error: ", self.getpeername(), exc_info=exc)

    def graceful_close(self):
        self._transport.pause_reading()
        self.should_close = True

    async def wait_closed(self):
        tasks = self._running_tasks
        pending = self._pending_message
        if pending:
            while pending:
                msg = pending.popleft()
                self._consume_one_message(msg)

        while tasks:
            task = tasks.pop()
            await task

    # others

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self.configuration.loop

    def getpeername(self):
        if not self._transport:
            return
        return self._transport.get_extra_info("peername")

    def __str__(self):
        return f"<{self.__class__.__name__} transport={self._transport} peer={self.getpeername()}>"

    __repr__ = __str__

    def __getstate__(self):
        raise TypeError("Cannot serialize connection object")
