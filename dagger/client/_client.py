import socket
from typing import Optional, List

from dagger.declare import Declare
from dagger.client._configuration import ClientConfiguration
from dagger.client._syncpool import SyncPool, BasePool
from dagger.client._request import Request

__all__ = ("Client",)


class Client:
    request_class = Request

    def __init__(self):
        self._configuration: Optional[ClientConfiguration] = None
        self._pool: Optional[BasePool] = None
        self._setdeclare: List[Declare] = []

    def dispatch_request(self, method: str, args=()):
        if self._pool is None:
            raise RuntimeError("not initialized")

        request = self.request_class(method, args)
        max_retry_time = self._configuration.max_retry
        if max_retry_time != 0:
            while True:
                try:
                    return self._pool.dispatch_request(request)
                except (ConnectionError, socket.error, TimeoutError, socket.timeout):
                    max_retry_time -= 1
                    if max_retry_time == 0:
                        raise
        else:
            return self._pool.dispatch_request(request)

    def setup(self, configuration: ClientConfiguration):
        self._configuration = configuration
        if configuration.asynchronous:
            from dagger.client._asyncpool import AsyncPool

            self._pool = AsyncPool(self._configuration)
        else:
            self._pool = SyncPool(self._configuration)

        for declare in configuration.declares:
            declare.setdefault_client(self)
            self._setdeclare.append(declare)

    def __del__(self):
        for declare in self._setdeclare:
            declare.setdefault_client(None)
        self._setdeclare = None
        self._pool = None
        self._configuration = None
