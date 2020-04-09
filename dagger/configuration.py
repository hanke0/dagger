import argparse
from typing import Tuple, Iterable, Mapping

__all__ = ("ConfigBase", "make_property", "int_format", "instance_checker", "float_format")


class ConfigurableProperty(property):
    configurable = True

    def __init__(self, fget, fset=None, fdel=None, doc=None, parser_options: Mapping = None):
        super().__init__()
        self.__module__ = fget.__module__
        self.__doc__ = doc or fget.__doc__
        self._fget = fget
        self._fset = fset
        self._fdel = fdel
        self.parser_options = parser_options

    def __set__(self, obj, value):
        if self._fset is None:
            raise AttributeError(f"can't set attribute")
        self._fset(obj, value)

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        return self._fget(obj)

    def __delete__(self, obj):
        if self._fdel is None:
            raise AttributeError("can't del attribute")
        self._fdel(obj)


def make_property(
    name: str,
    formatter: callable = None,
    doc: str = None,
    default=None,
    configurable: bool = True,
    parser_options: Mapping = None,
):
    if not name.isidentifier():
        raise RuntimeError("invalid property name: %r" % name)

    variable = "_" + name

    def fget(self):
        if not hasattr(self, variable):
            if callable(default):
                v = default()
            else:
                v = default
            setattr(self, variable, v)

        return getattr(self, variable)

    def fset(self, v):
        if formatter:
            v = formatter(v)
        setattr(self, variable, v)

    def fdel(self):
        setattr(self, variable, None)

    if configurable:
        return ConfigurableProperty(fget, fset, fdel, doc, parser_options)
    return property(fget, fset, fdel, doc)


def int_format(max=None, min=None):
    def _format(v):
        try:
            v = int(v)
        except Exception:
            raise ValueError(v)
        if max is not None:
            if v > max:
                raise ValueError(f"{v} bigger than {max}")

        if min is not None:
            if v < min:
                raise ValueError(f"{v} is lower than {min}")
        return v

    return _format


def float_format(max=None, min=None):
    def _format(v):
        try:
            v = float(v)
        except Exception:
            raise ValueError(v)
        if max is not None:
            if v > max:
                raise ValueError(f"{v} bigger than {max}")

        if min is not None:
            if v < min:
                raise ValueError(f"{v} is lower than {min}")
        return v

    return _format


def instance_checker(*cls):
    def _format(v):
        if not isinstance(v, cls):
            raise ValueError(f"except type {cls}, got {type(v)}")
        return v

    return _format


class ConfigBase:
    @classmethod
    def register_to_argument_parser(cls, parser: argparse.ArgumentParser):
        for k, v in cls._list_configurable():
            name = "--" + k.replace("_", "-")
            if v.parser_options:
                parser.add_argument(name, dest=k, help=v.__doc__, **v.parser_options)
            else:
                parser.add_argument(name, dest=k, help=v.__doc__)

    @classmethod
    def _list_configurable(cls) -> Iterable[Tuple[str, ConfigurableProperty]]:
        for i in dir(cls):
            v = getattr(cls, i)
            if getattr(v, "configurable", False):
                yield i, v

    def set_from_namespace(self, ns: argparse.Namespace, empty=None):
        if hasattr(ns, "__getitem__"):
            get_attr = lambda obj, item: obj[item]
        else:
            get_attr = getattr

        for k, _ in self._list_configurable():
            if k in ns:
                v = get_attr(ns, k)
                if v is empty:
                    continue
                setattr(self, k, v)

    def __str__(self):
        a = (f"{k}={getattr(self, k)}" for k, _ in self._list_configurable())
        return f"Configuration({', '.join(a)})"

    __repr__ = __str__
