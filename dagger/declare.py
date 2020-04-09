import inspect
import enum
from functools import partial

from dagger.exceptions import FunctionNotImplementedError, ContentVerifyFailed

__all__ = ("Declare", "declare")


class RunMode(enum.IntEnum):
    THREAD_RUN = 0
    ASYNC_RUN = 1
    SYNC_RUN = 2


def _make_dummy(name):
    def _dummy_server(*args):
        raise FunctionNotImplementedError(name)

    return _dummy_server


class Declare:
    THREAD_RUN = RunMode.THREAD_RUN
    ASYNC_RUN = RunMode.ASYNC_RUN
    SYNC_RUN = RunMode.SYNC_RUN
    _DUMMY = False

    def __init__(self, name: str, module: str, doc: str, signature: inspect.Signature):
        self.name = name
        self.__name__ = name
        self.__module__ = module
        self.__doc__ = doc
        dummy = _make_dummy(name)
        self._server_impl = dummy
        self._client = None
        self._signature = signature
        self.runmode = self.THREAD_RUN
        self._parameter_check = None
        if not self._DUMMY:
            self._check_args()

    def set_server_impl(self, func, thread=None, asynchronous=None):
        assert not all((thread, asynchronous))
        if thread is None and asynchronous is None:
            thread = True
            asynchronous = False
        if thread:
            self.runmode = self.THREAD_RUN
        elif asynchronous:
            self.runmode = self.ASYNC_RUN
        else:
            self.runmode = self.SYNC_RUN

        self._server_impl = func

    def server_impl(self, func=None, *, thread=None, asynchronous=None):
        if func is None:
            return partial(self.server_impl, thread=thread, asynchronous=asynchronous)
        self.set_server_impl(func, thread, asynchronous)
        return func

    def assured_parameters(self, *args, **kwargs):
        bond_args: inspect.BoundArguments = self._signature.bind(*args, **kwargs)
        bond_args.apply_defaults()
        args = bond_args.args
        if self._parameter_check:
            args = self._parameter_check(*args)
        return args

    def parameters_assure(self, func):
        self._parameter_check = func
        return func

    def dispatch_request(self, client, args=(), kwargs=None):
        if kwargs:
            args = self.assured_parameters(*args, **kwargs)
        else:
            args = self.assured_parameters(*args)

        return client.dispatch_request(self.name, args)

    def server_call(self, *args):
        try:
            bond_args: inspect.BoundArguments = self._signature.bind(*args)
            bond_args.apply_defaults()
        except TypeError as e:
            raise ContentVerifyFailed(e)
        else:
            return self._server_impl(*bond_args.args)

    def setdefault_client(self, client):
        self._client = client

    def remote_call(self, *args, **kwargs):
        if self._client is None:
            raise RuntimeError("client is not set")
        return self.dispatch_request(self._client, args, kwargs)

    def __call__(self, *args, **kwargs):
        raise RuntimeError("declare is not callable, use server_call or async_call or sync_call")

    def __str__(self):
        return "<%s name='%s' runmode=%s>" % (self.__class__.__name__, self.__name__, self.runmode)

    __repr__ = __str__

    @classmethod
    def make_dummy(cls, name):
        dummy_sig = inspect.signature(_make_dummy(name))
        return _DummyDeclare(name, cls.__module__, "dummy declare", dummy_sig)

    _DISALLOW_KIND = (
        inspect.Parameter.VAR_POSITIONAL,
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.VAR_KEYWORD,
    )

    def _check_args(self):
        for k, v in self._signature.parameters.items():
            if v.kind in self._DISALLOW_KIND:
                raise TypeError(
                    f"Declared function({self.name}) could only accept positional parameter."
                )


class _DummyDeclare(Declare):
    _DUMMY = True


def declare(func) -> Declare:
    name = func.__name__
    signature = inspect.signature(func)
    module = func.__module__
    doc = func.__doc__
    return Declare(name, module, doc, signature)


del RunMode
