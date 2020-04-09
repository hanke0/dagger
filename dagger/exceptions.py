from typing import Type


class DaggerError(Exception):
    default_message = ""
    message_format = None
    code = 100

    def __init__(self, message=None, caught_by=None):
        if message is None and caught_by is None:
            message = self.default_message
        elif message:
            message = message
        else:
            message = repr(caught_by)

        self.caught_by = caught_by
        if self.message_format is not None:
            message = self.message_format % message
        super().__init__(message)


class ContentVerifyFailed(DaggerError):
    message_format = "Invalid Content: %s"
    code = 402


class FunctionNotImplementedError(DaggerError):
    message_format = "function not implemented: %r"
    code = 404


class RemoteInternalError(DaggerError):
    message_format = "Internal Error: %s"
    code = 500


class FrameError(DaggerError):
    message_format = "Invalid Frame: %s"
    code = 509


class PackUnpackError(DaggerError):
    default_message = "unknown"
    message_format = "Can't pack or unpack body because of %s"
    code = 510


_code_err_map = {}


def _setup_code_map(d):
    rv = {}
    for k, v in d.items():
        if isinstance(v, DaggerError):
            rv[v.code] = v

    _code_err_map.update(rv)


_setup_code_map(globals())
del _setup_code_map


def get_exception_from_code(code, default=DaggerError) -> Type[DaggerError]:
    return _code_err_map.get(code, default=default)
