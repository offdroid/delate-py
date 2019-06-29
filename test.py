import socket

import asyncio
from delate import connection


async def main():
    while True:
        try:
            async with connection.Connection() as c:
                print("connected")

                async with connection.Subscription(c, ["timetable_8000261"]) as s:
                    async for m in s:
                        print(m.raw_str)
                        await c.close()
        except socket.gaierror as e:
            print(f"Connection failed: {e}")

        print("Restarting ...")
        await asyncio.sleep(5)

    asyncio.get_event_loop().stop()


asyncio.get_event_loop().create_task(main())
asyncio.get_event_loop().run_forever()
