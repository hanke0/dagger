from threading import local
from typing import Sequence

from dagger.codec import pack_message, EventType, MAX_SEQUENCE_ID

__all__ = ("Request",)


def _seq_gen_func():
    while True:
        for i in range(MAX_SEQUENCE_ID):
            yield i


_local = local()


def next_sequence_id():
    seq_gen = getattr(_local, "seq_gen", None)
    if seq_gen is None:
        seq_gen = _local.seq_gen = _seq_gen_func()

    return seq_gen.send(None)


class Request:
    _missing = object()
    _event_type = EventType.REQUEST.value

    __slots__ = ("_method", "_parameters", "_sequence_number")

    def __init__(self, method: str, parameters: Sequence):
        # parameters should be checked before
        self._method = method
        self._parameters = parameters
        self._sequence_number = next_sequence_id()

    def pack(self):
        return pack_message(
            self._sequence_number, self._event_type, [self._method, self._parameters]
        )

    @property
    def sequence_number(self):
        return self._sequence_number

    @property
    def method(self) -> Sequence:
        return self._method

    @property
    def parameters(self) -> Sequence:
        return self._parameters

    def __str__(self):
        return (
            f"<{self.__class__.__name__} "
            f"method={self.method} "
            f"parameters={self.parameters} "
            f"sequence-number={self.sequence_number}>"
        )
