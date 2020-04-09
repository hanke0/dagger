# Dagger

## Description

This project is a python RPC framework using asyncio and msgpack.

### Features

* Base on asyncio, and support both sync functions and async functions in both server side and client side.
* Pay attention to the balance between performance and ease of use.
* Declare API, then you can easily use it in client. Only care about the realization of the business in server side.
* Pandas and numpy data support.
* Multiprocess worker management in server side, including memory limiting and restart on error.


## How to use

A simple example code like:
```python
# declare.py
from dagger.declare import declare

@declare
def hello_world(n=0):
    pass


# server.py
import asyncio
from declare import hello_world

@hello_world.server_impl(asynchronous=True)
def hello_world_async_impl(n):
    return sleep(n)

async def sleep(n=1):
    await asyncio.sleep(n)
    return "hello async world!"

# client.py

from dagger.client import ClientConfiguration, Client
from declare import hello_world

configuration = ClientConfiguration()
configuration.asynchronous = False
configuration.register_declares_from_module("declare")
client = Client()
client.setup(configuration)


print(hello_world.remote_call())

```

Start a server by command `python -m dagger.server --module server`. Now you can run `python client.py`
to get a hello world message.

You can find a example in directory `./tools` wrote for testing.


## License

This project is under MIT license, detail information can be found in the COPYING file.
