import io
import socket
from queue import Queue, Empty, Full
from typing import NamedTuple

from dagger.netutils import create_default_connection
from dagger.client._configuration import ClientConfiguration
from dagger.client._request import Request
from dagger.codec import decode_header, EventType, unpack_payload
from dagger.exceptions import FrameError, get_exception_from_code


class BasePool:
    request_class = Request

    def __init__(self, configuration: ClientConfiguration):
        self._configuration = configuration

    def dispatch_request(self, request: Request):
        raise NotImplementedError


class _BufferSocket(NamedTuple):
    buffer: io.BufferedRWPair
    socket: socket.socket


class SyncPool(BasePool):
    def __init__(self, configuration: ClientConfiguration):
        super().__init__(configuration)
        self._conns = Queue(configuration.pool_size)
        conn = self._make_new_connection()
        self._conns.put(conn)

    def dispatch_request(self, request: Request):
        #  parameters should be checking in declare
        try:
            conn = self._conns.get_nowait()
        except Empty:
            conn = self._make_new_connection()
        else:
            if conn.buffer.closed:
                conn = self._make_new_connection()

        try:
            rv = self._dispatch_request(conn, request)
        except (ConnectionError, socket.error, TimeoutError, socket.timeout):
            conn.buffer.close()
            conn.socket.close()
            raise
        else:
            try:
                self._conns.put_nowait(conn)
            except Full:
                conn.buffer.close()
                conn.socket.close()
        if isinstance(rv, Exception):
            raise rv
        return rv

    def _make_new_connection(self):
        host = self._configuration.host
        port = self._configuration.port
        timeout = self._configuration.timeout
        sock = create_default_connection(host, port, timeout=timeout)
        buffer = sock.makefile("rwb", 4096)
        buffersocket = _BufferSocket(buffer, sock)
        return buffersocket

    @staticmethod
    def _dispatch_request(conn: _BufferSocket, request: Request):
        conn.buffer.write(request.pack())
        conn.buffer.flush()
        headerbytes = conn.buffer.read(8)
        if not headerbytes:
            raise ConnectionError(f"{conn.socket} lost")
        header = decode_header(headerbytes)
        if header.event_type != EventType.RESPONSE:
            raise FrameError("expect %s, got %d" % (EventType.RESPONSE, header.event_type))

        payload = conn.buffer.read(header.payload_size)
        if not headerbytes:
            raise ConnectionError(f"{conn.socket} lost")

        body = unpack_payload(header.compress_flag, payload)

        if header.errno:
            exc = get_exception_from_code(header.errno)
            body = exc(body)

        return body
