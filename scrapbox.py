import asyncio
import couchbase.experimental
couchbase.experimental.enable()
from acouchbase.bucket import Bucket
from sanic.response import json
from sanic import Sanic

app = Sanic(__name__)

bucket = None

async def connect(*args):
    global bucket
    connection_string = 'couchbase://localhost/porcupine'
    bucket = Bucket(connection_string,
                    password='')
    await bucket.connect()


@app.route('/')
async def hello(request):
    # print(os.getpid())
    await bucket.get('system')
    await bucket.get('users')  # , 'p_id', 'acl', 'deleted', 'sys')
    return json(await bucket.get('admin'))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app.run(workers=2, before_start=[connect])
