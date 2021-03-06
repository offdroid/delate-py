import asyncio
import json
import socket

from delate import connection


async def main():
    try:
        while True:
            try:
                async with connection.Connection() as conn:
                    print("Connected")

                    async with connection.Subscription(
                        conn, ["timetable_8000261"]
                    ) as sub:
                        async for msg in sub:
                            print(json.dumps(msg.content, indent=4, sort_keys=True))
                            await conn.close()
            except socket.gaierror as e:
                print(f"Connection failed: {e}")

            print("Restarting ...")
            await asyncio.sleep(5)
    finally:
        asyncio.get_event_loop().stop()


asyncio.get_event_loop().create_task(main())
asyncio.get_event_loop().run_forever()
