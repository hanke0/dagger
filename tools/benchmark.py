import os
import sys

import asyncio
import time
import threading
import importlib

import gevent
import gevent.socket

if __name__ == "__main__":
    # Make sure we could import dagger from project root path.
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dagger.codec import pack_message, EventType, decode_header, unpack_payload

tlock = threading.Lock()
tcount = 0
ttime = 0.0
first = True


def print_result(spend, total_request):
    print("total flow time: %.08f" % spend)
    print("total request:", total_request)
    print("finish request:", tcount)
    print("cost time: %.08f" % ttime)
    if tcount != 0:
        print("average time: %.08f" % (ttime / tcount))
    if spend != 0:
        print("qps: %.08f" % (tcount / spend))


def next_event():
    while True:
        for i in range(2 ** 16 - 1):
            yield i


def raw_job(loop):
    count = 0
    timer = 0
    global tcount, ttime, first
    try:
        gene = next_event()
        s = gevent.socket.create_connection(("127.0.0.1", 10050))
        for i in range(loop):
            buffer = s.makefile("rb", 1024)
            pack = pack_message(gene.send(None), EventType.REQUEST.value, [run_declare.name, [0]])
            s.sendall(pack)
            start = time.time()
            header = buffer.read(8)
            header = decode_header(header)
            body = buffer.read(header.payload_size)
            end = time.time()
            body = unpack_payload(header.compress_flag, body)
            if first:
                print(body)
                first = False

            assert isinstance(body, str)
            timer += end - start
            count += 1
    finally:
        with tlock:
            tcount += count
            ttime += timer


def raw_benchmark(concurrent, loop):
    start = time.time()
    try:
        jobs = []
        for _ in range(concurrent):
            t = gevent.spawn(raw_job, loop)
            jobs.append(t)

        start = time.time()
        gevent.joinall(jobs, timeout=None)
    finally:
        end = time.time()
        spend = end - start
        print_result(spend, concurrent * loop)


async def async_job(loop):
    count = 0
    timer = 0
    global tcount, ttime, first
    try:
        for i in range(loop):
            start = time.time()
            body = await run_declare.remote_call()
            if first:
                print(body)
                first = False
            end = time.time()
            count += 1
            timer += end - start
    finally:
        with tlock:
            tcount += count
            ttime += timer


def async_benchmark(concurrent, loop):
    from dagger.client import ClientConfiguration, Client

    configuration = ClientConfiguration()
    configuration.host = "127.0.0.1"
    configuration.port = 10050
    configuration.asynchronous = True
    configuration.pool_size = 120000
    configuration.register_declares_from_module("hello_world")
    client = Client()
    client.setup(configuration)
    start = time.time()
    eventloop = configuration.loop
    try:
        jobs = []
        for _ in range(concurrent):
            t = eventloop.create_task(async_job(loop))
            jobs.append(t)

        start = time.time()

        gather = asyncio.gather(*jobs)
        eventloop.run_until_complete(gather)

    finally:
        end = time.time()
        spend = end - start
        print_result(spend, concurrent * loop)


def sync_job(loop):
    count = 0
    timer = 0
    global tcount, ttime, first
    try:
        for i in range(loop):
            start = time.time()
            body = run_declare.remote_call()
            if first:
                print(body)
                first = False
            end = time.time()
            count += 1
            timer += end - start
    finally:
        global tcount, ttime
        with tlock:
            tcount += count
            ttime += timer


def sync_benchmark(concurrent, loop):
    import gevent.monkey

    gevent.monkey.patch_all()
    from dagger.client import ClientConfiguration, Client

    configuration = ClientConfiguration()
    configuration.asynchronous = False
    configuration.register_declares_from_module("hello_world")
    client = Client()
    client.setup(configuration)
    start = time.time()
    del configuration.loop
    try:
        jobs = []
        for _ in range(concurrent):
            t = gevent.spawn(sync_job, loop)
            jobs.append(t)

        start = time.time()
        gevent.joinall(jobs, timeout=None)

    finally:
        end = time.time()
        spend = end - start
        print_result(spend, concurrent * loop)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--concurrent", dest="concurrent", type=int, default=1)
    parser.add_argument("-l", "--loop", dest="loop", type=int, default=1)
    parser.add_argument(
        "-m", "--mode", dest="mode", choices=["sync", "async", "raw"], default="raw"
    )
    parser.add_argument(
        "-d",
        "--declare",
        dest="declare",
        default="hello_world:hello_world_sync",
        help="declare import path",
    )
    args = parser.parse_args()
    declare: str = args.declare
    module, declare = declare.split(":", 1)
    module = importlib.import_module(module)
    request = getattr(module, declare)
    run_declare = request

    concurrent = args.concurrent
    loop = args.loop
    mode = args.mode
    if mode == "raw":
        raw_benchmark(concurrent, loop)
    elif mode == "sync":
        sync_benchmark(concurrent, loop)
    elif mode == "async":
        async_benchmark(concurrent, loop)
    else:
        exit(1)
