import asyncio
import time
import os
import sys

if __name__ == "__main__":
    # Make sure we could import dagger from project root path.
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dagger.declare import declare


@declare
def hello_world_thread(sleep=0):
    """hello world thread"""
    pass


@hello_world_thread.server_impl(thread=True)
def hello_world_thread_impl(sleep):
    if sleep:
        time.sleep(sleep)
    return "hello thread world!"


@declare
def hello_world_async(sleep=0):
    """hello world async"""
    pass


@hello_world_async.server_impl(asynchronous=True)
def hello_world_async_impl(n):
    return sleep(n)


async def sleep(n=10):
    if n:
        await asyncio.sleep(n)
    return "hello async world!"


@declare
def hello_world_sync(sleep=0):
    pass


@hello_world_sync.server_impl(thread=False)
def hello_world_sync_impl(n):
    if n:
        time.sleep(n)
    return "hello sync world!"


if __name__ == "__main__":
    import sys
    from dagger.server import start_from_console

    argv = "--module hello_world --server-name test-rpc --backlog 100".split()
    if len(sys.argv) != 1:
        argv.extend(sys.argv[1:])

    start_from_console(argv=argv)
