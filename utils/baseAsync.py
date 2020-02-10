import asyncio

from tornado.ioloop import IOLoop


class BaseAsync():
    def __init__(self):
        self.ioloop = IOLoop.current()
        self.aioloop = asyncio.get_event_loop()