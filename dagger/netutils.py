import socket
import fcntl
import os
import logging
from urllib.parse import urlparse, parse_qsl
from typing import NamedTuple
from argparse import Namespace as _Namespace

__all__ = ("resolve_uri", "resolve_query", "create_default_connection", "create_default_listener")

logger = logging.getLogger(__name__)


class ResolveURIResult(NamedTuple):
    scheme: str
    host: str
    port: int
    username: str
    password: str
    path: str
    query: str


class Namespace(_Namespace):
    def __init__(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)

    def __repr__(self):
        type_name = type(self).__name__
        arg_strings = []
        for name, value in self._get_kwargs():
            if name.isidentifier():
                arg_strings.append("%s=%r" % (name, value))
        return "%s(%s)" % (type_name, ", ".join(arg_strings))

    def _get_kwargs(self):
        return sorted(self.__dict__.items())

    def __eq__(self, other):
        if not isinstance(other, Namespace):
            return NotImplemented
        return vars(self) == vars(other)

    def __contains__(self, key):
        return key in self.__dict__


def resolve_uri(uri: str) -> ResolveURIResult:
    r = urlparse(uri)
    return ResolveURIResult(r.scheme, r.hostname, r.port, r.username, r.password, r.path, r.query)


def resolve_query(query: str) -> Namespace:
    rv = Namespace(parse_qsl(query, keep_blank_values=True))
    return rv


def create_default_connection(
    host: str, port: int, connect_timeout: int = 30, timeout: int = 30
) -> socket.socket:
    sock = socket.create_connection((host, port), connect_timeout)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
    sock.settimeout(timeout)
    return sock


def create_default_listener(
    host: str, port: int, timeout: int = 30, blocking=True
) -> socket.socket:
    if ":" in host:
        family = socket.AF_INET6
    else:
        family = socket.AF_INET

    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
    sock.settimeout(timeout)
    sock.setblocking(blocking)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    else:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if not blocking:
        try:
            flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFD) | os.O_NONBLOCK
            fcntl.fcntl(sock.fileno(), fcntl.F_SETFD, flags)
        except OSError as e:
            logger.warning(f"call fcntl O_NONBLOCK on listener raise {e:r}")

    sock.bind((host, port))
    return sock
