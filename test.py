import asyncio
from delate import connection


async def main():
    async with connection.Connection() as c:
        print("connected")

        async with connection.Subscription(c, "timetable_8000261") as s:
            i = 0
            async for m in s:
                print(m.raw_str)
                i += 1
                if i > 5:
                    break  # unsubscribes


asyncio.get_event_loop().create_task(main())
asyncio.get_event_loop().run_forever()
