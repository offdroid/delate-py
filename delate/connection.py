import asyncio
import logging

import websockets

from delate.message import Message

ENDPOINT_URL = "wss://tralis.sbahnm.geops.de/ws"


class Connection:
    """A (stateful) connection to the Redis WebSocket API."""

    def __init__(self):
        self.ws = None

        self.subs = {}

        self._remote_subs = set()

        self.connected = asyncio.Event()
        self.tasks = []

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self):
        self.ws = await websockets.connect(ENDPOINT_URL)

        loop = asyncio.get_event_loop()
        self.tasks = [loop.create_task(self.ping()), loop.create_task(self.recv())]

        self.connected.set()
        return self

    async def close(self):
        self.connected.clear()
        await self.ws.close()

        for task in self.tasks:
            task.cancel()

    async def wait_closed(self):
        await self.ws.wait_closed()

    async def ping(self):
        while True:
            await self.connected.wait()
            while not self.ws.closed:
                await asyncio.sleep(10)
                await self.send("PING")

    async def recv(self):
        while True:
            await self.connected.wait()
            try:
                async for message_str in self.ws:
                    if isinstance(message_str, str):
                        message = Message(message_str)

                        for source in {message.source, None}:
                            channel = self._get_channel_queue(source, create=False)
                            if channel is not None:
                                for queue in channel:
                                    queue.put_nowait(message)
                    else:
                        logging.warning("Unsupported binary message received")
            except websockets.exceptions.ConnectionClosed as e:
                await self.close()

    async def send(self, message):
        try:
            await self.ws.send(message)
        except websockets.exceptions.ConnectionClosed as e:
            await self.close()

    async def subscribe_queue(self, queue, channels=None, get=False):
        for channel in self._get_channel_defs(channels):
            self._get_channel_queue(channel).append(queue)
            await self._subscribe_remote(channel, get)

    async def unsubscribe_queue(self, queue):
        for channel, channel_queues in self.subs.items():
            if queue in channel_queues:
                channel_queues.remove(queue)

                if len(channel_queues) == 0:
                    await self._unsubscribe_remote(channel)

    def _get_channel_defs(self, channels_def):
        if channels_def is None or isinstance(channels_def, str):
            yield channels_def
        else:
            yield from channels_def

    def _get_channel_queue(self, channel, create=True):
        if channel not in self.subs:
            if create:
                self.subs[channel] = []
            else:
                return None
        return self.subs[channel]

    async def _subscribe_remote(self, channel, get=False):
        if channel is not None and channel not in self._remote_subs:
            if get:
                await self.send(f"GET {channel}")
            await self.send(f"SUB {channel}")
            self._remote_subs.add(channel)

    async def _unsubscribe_remote(self, channel):
        if channel is not None and channel in self._remote_subs:
            await self.send(f"DEL {channel}")
            self._remote_subs.remove(channel)


class Subscription:
    """An iterator wrapped over one or more subscription queue."""

    def __init__(self, connection, channels, get=False):
        self.connection = connection
        self.channels = channels
        self.get = get

        self.queue = asyncio.Queue()

    async def __aenter__(self):
        await self.connection.subscribe_queue(self.queue, self.channels, self.get)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.connection.unsubscribe_queue(self.queue)

    def __aiter__(self):
        return self

    async def __anext__(self):
        get_task = asyncio.ensure_future(self.queue.get())
        err_task = asyncio.ensure_future(self.connection.wait_closed())

        done, pending = await asyncio.wait(
            [get_task, err_task], return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()

        if err_task in done:
            raise StopAsyncIteration
        return get_task.result()

    def __aclose__(self):
        self.connection.unsubscribe_queue(self.queue).__await__()
