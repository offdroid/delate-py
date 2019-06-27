import asyncio
import websockets

from delate.message import Message


ENDPOINT_URL = "wss://tralis.sbahnm.geops.de/ws"


class Connection:
    """ A connection to the MVV API """

    def __init__(self):
        self.ws = None

        loop = asyncio.get_event_loop()
        loop.create_task(self.ping())

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def connect(self):
        self.ws = await websockets.connect(ENDPOINT_URL)
        return self

    async def close(self):
        self.ws.close()

    async def ping(self):
        while True:
            await asyncio.sleep(5)

            if self.ws.open:
                await self.ws.send("PING")
                print("ping sent")

    async def recv(self):
        async for message_str in self.ws:
            message = Message(message_str)
            yield message
