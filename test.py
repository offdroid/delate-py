import asyncio
from delate import connection


async def main():
    async with connection.Connection() as c:
        print("connected")
        async for message in c.recv():
            print(message.raw_str)
        print("end")


asyncio.get_event_loop().create_task(main())
asyncio.get_event_loop().run_forever()
