import struct
import threading
import time
import os
from random import SystemRandom
from typing import NamedTuple, Type

__all__ = ("get_unique_id", "SimpleSignal")


class _IdGenerator:
    """mongodb ObjectId compatible ID generator"""

    def __init__(self):
        self._pid = os.getpid()
        self._max = 0xFFFFFF
        self._max_flag = self._max - 1
        self._count = SystemRandom().randint(0, self._max)
        self._mutex = threading.Lock()
        self._process_identifier = os.urandom(5)

    def _get_process_identifier(self):
        pid = os.getpid()
        if self._pid != pid:
            self._pid = pid
            self._process_identifier = os.urandom(5)
        return self._process_identifier

    def get_unique_id(self) -> bytes:
        # 4 bytes current time
        id_ = struct.pack(">I", int(time.time()))

        # 5 bytes process identifier
        id_ += self._get_process_identifier()

        # 3 bytes inc
        with self._mutex:
            count = self._count
            id_ += struct.pack(">I", self._count)[1:4]
            self._count = (count + 1) & self._max_flag
        return id_


_default_id_generator = _IdGenerator()

get_unique_id = _default_id_generator.get_unique_id


class SimpleSignal:
    def __init__(self, scope: Type[NamedTuple]):
        """
        Create a new signal.

        providing_args
            A list of the arguments this signal can pass along in a notify() call.
        """
        self.receivers = []
        if scope is None:
            scope = []
        self.scope_class: Type[NamedTuple] = scope

    def connect(self, receiver):
        self.receivers.append(receiver)

    def disconnect(self, receiver):
        disconnected = 0
        for recv in self.receivers:
            if recv == receiver:
                disconnected += 1
        return disconnected

    def has_listeners(self):
        return bool(self.receivers)

    def notify(self, scope):
        if not self.receivers:
            return []
        assert isinstance(scope, self.scope_class)

        return [(receiver, receiver(scope)) for receiver in self.receivers]

    def notify_robust(self, scope):
        if not self.receivers:
            return []
        assert isinstance(scope, self.scope_class)
        responses = []
        for receiver in self.receivers:
            try:
                response = receiver(scope)
            except Exception as err:
                responses.append((receiver, err))
            else:
                responses.append((receiver, response))
        return responses
