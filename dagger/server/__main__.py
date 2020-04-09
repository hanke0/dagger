import asyncio
import argparse
import importlib
import logging

from dagger.server import ServerConfiguration
from dagger.declare import Declare
from dagger.logger import setup_logging
from dagger.supervisor import Supervisor

__all__ = ("start_from_console",)

_print_log = logging.getLogger("_print_log")
_print_log.propagate = False
_print_log.handlers.clear()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)
_print_log.addHandler(handler)
del formatter, handler
_print_log.setLevel(logging.DEBUG)


def print_(*args, **kwargs):
    spe = kwargs.get("spe", " ")
    _print_log.info(spe.join(str(i) for i in args))


def start_from_console(configuration: ServerConfiguration = None, argv=None):
    if not configuration:
        configuration = ServerConfiguration()

    parser = argparse.ArgumentParser("dagger")
    configuration.register_to_argument_parser(parser)
    parser.add_argument("--module", dest="module", help="declare module", required=True)
    parser.add_argument("--use-uvloop", action="store_true", dest="use_uvloop", default=False)
    args = parser.parse_args(args=argv)
    if args.use_uvloop:
        import uvloop

        uvloop.install()
        print_("UVLOOP INSTALLED")

    configuration.set_from_namespace(args)

    setup_logging(configuration.log_level)

    host = configuration.host
    port = configuration.port
    print_(f"start server on {host}:{port}")
    module = importlib.import_module(args.module)
    print_("load module:", f"'{args.module}'", "at path", f"'{module.__file__}'")
    declares = []
    for varname in dir(module):
        varval = getattr(module, varname)
        if isinstance(varval, Declare):
            declares.append(varval)
    configuration.register_declares(*declares)

    if declares:
        print_(f"Declares: ")
        for d in declares:
            print_(f"\t - {d}")
    else:
        print_("No Declares.\n")

    def target():
        configuration.loop = asyncio.get_event_loop()
        server = configuration.make_server()
        server.server_forever()

    if configuration.server_name:

        def name():
            return f"{configuration.server_name}:{configuration.port}"

    else:
        name = None

    manager = Supervisor(target, name=name, worker_memory_limit=configuration.worker_memory_limit)
    manager.start(configuration.process)


if __name__ == "__main__":
    start_from_console()
