import couchbase
import asyncio
import uvloop
import ujson
from timeit import timeit
import couchbase.experimental
import couchbase.subdocument as SD
couchbase.experimental.enable()
from acouchbase.bucket import Bucket as aBucket
from couchbase.bucket import Bucket
import couchbase.subdocument as SD

asyncio.set_event_loop(uvloop.new_event_loop())
couchbase.set_json_converters(ujson.dumps, ujson.loads)

async def connect_async():
    c = aBucket('couchbase://localhost/porcupine')
    await c.connect()
    return c


def connect():
    c = Bucket('couchbase://localhost/porcupine')
    return c


items = {
    'test': {'x': 'tassosαααα',
             'y': 2},
    'test2': 'ttttt'
}


def test(b):
    # for i in range(10):
    for i in range(1000):
        # b.upsert_multi(items, format=couchbase.FMT_AUTO)
        # b.mutate_in('test', SD.upsert('z', 1))
        # b.append('test2', ' Koutsovassilisσσσσσσ')
        value = b.get('ROOT')
    # print(value)


async def test_async(b):
    # for i in range(10):
    for i in range(2000):
        # inserts = b.upsert_multi(items, format=couchbase.FMT_AUTO)
        # mutations = b.mutate_in('test', SD.upsert('z', 'async'))
        # appends = b.append('test2', ' Koutsovassilisσσσσσσ')
        # completed, pending = await asyncio.wait([inserts, mutations, appends])
        value = (await b.retrieve_in('ROOT', 'security'))
    for x in value:
        print(x)

loop = asyncio.get_event_loop()
abucket = loop.run_until_complete(connect_async())
bucket = connect()

t = timeit('test(bucket)', number=1, globals=globals())
print('normal', t)

t = timeit('loop.run_until_complete(test_async(abucket))', number=1,
           globals=globals())
print('async', t)
