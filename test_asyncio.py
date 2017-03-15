import asyncio

from porcupine.core.aiolocals.local import Local, Context


class Sleeper:
    def __init__(self, seconds):
        self.sec = seconds

    async def sleep(self):
        print(foo.bar)
        await asyncio.sleep(self.sec)
        return True


# @asyncio.coroutine
async def my_coroutine(seconds_to_sleep=3):
    print(foo)
    with Context():
        foo.bar = "baz.{}".format(seconds_to_sleep)
        a = Sleeper(seconds_to_sleep)
        return a.sleep()


foo = Local()

# print(asyncio.get_event_loop.__qualname__)

loop = asyncio.get_event_loop()
# # my_local = local(loop=loop)

print(await my_coroutine(3))
# loop.run_until_complete(
#     asyncio.wait([my_coroutine(4), my_coroutine(3)])
# )
loop.close()


