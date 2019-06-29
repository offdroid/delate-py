import asyncio
import websockets

from delate.message import Message


ENDPOINT_URL = "wss://tralis.sbahnm.geops.de/ws"


class Connection:
    """ A (stateful) connection to the Redis WebSocket API """

    def __init__(self):
        self.ws = None

        self.subs = {}

        self._remote_subs = set()

        self.connected = asyncio.Event()

        loop = asyncio.get_event_loop()
        loop.create_task(self.ping())
        loop.create_task(self.recv())

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self):
        self.ws = await websockets.connect(ENDPOINT_URL)
        self.connected.set()
        return self

    async def close(self):
        self.ws.close()
        self.connected.clear()

    async def ping(self):
        await self.connected.wait()
        while True:
            await asyncio.sleep(5)
            await self.send("PING")

    async def recv(self):
        while True:
            await self.connected.wait()
            async for message_str in self.ws:
                message = Message(message_str)

                for source in {message.source, None}:
                    channel = self._get_channel_queue(source, create=False)
                    if channel is not None:
                        for queue in channel:
                            queue.put_nowait(message)

    async def send(self, message):
        if self.ws.open:
            await self.ws.send(message)
            print("sent: " + message)

    async def subscribe_queue(self, queue, channels=None):
        for channel in self._get_channel_defs(channels):
            self._get_channel_queue(channel).append(queue)
            await self._subscribe_remote(channel)

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

    async def _subscribe_remote(self, channel):
        if channel is not None and channel not in self._remote_subs:
            await self.send(f"SUB {channel}")
            self._remote_subs.add(channel)

    async def _unsubscribe_remote(self, channel):
        if channel is not None and channel in self._remote_subs:
            await self.send(f"DEL {channel}")
            self._remote_subs.remove(channel)


class Subscription:
    """ An iterator wrapped over one or more subscription queue """
    def __init__(self, connection, channels):
        self.connection = connection
        self.channels = channels

        self.queue = asyncio.Queue()

    async def __aenter__(self):
        await self.connection.subscribe_queue(self.queue, self.channels)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.connection.unsubscribe_queue(self.queue)

    async def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.queue.get()

