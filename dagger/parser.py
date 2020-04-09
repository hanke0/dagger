from typing import Any

__all__ = ("Parser", "ParserProtocol")


class ParserProtocol:
    header_size: int

    def parse_header(self, data: bytes) -> (int, Any):
        raise NotImplementedError

    def parse_payload(self, header, data: bytes):
        raise NotImplementedError

    def on_message_complete(self, header, body):
        raise NotImplementedError


class Parser:
    __slots__ = ("_protocol", "_buffer", "_exception", "_next_read_size", "_current_header")

    def __init__(self, protocol: ParserProtocol):
        self._protocol = protocol
        self._buffer = bytearray()
        self._exception = None
        self._next_read_size = protocol.header_size
        self._current_header = None

    def feed_data(self, data: bytes):
        if self._exception:
            raise self._exception

        buffer = self._buffer
        buffer.extend(data)
        next_read = self._next_read_size
        if next_read > len(buffer):
            return
        current_header = self._current_header
        protocol = self._protocol
        read = self._read_from_buffer
        try:
            while not (len(buffer) < next_read):
                data = read(next_read)
                if current_header is None:
                    next_read, current_header = protocol.parse_header(data)
                else:
                    body = protocol.parse_payload(current_header, data)
                    protocol.on_message_complete(current_header, body)
                    next_read = protocol.header_size
                    current_header = None
        except Exception as exc:
            # clean buffer
            self._buffer = None
            self._exception = exc
            raise
        else:
            self._next_read_size = next_read
            self._current_header = current_header

    def _read_from_buffer(self, n: int) -> bytes:
        if n <= 0:
            return b""

        data = self._buffer[:n]
        del self._buffer[:n]
        return data
