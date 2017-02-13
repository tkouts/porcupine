from sanic import Sanic
from sanic.response import json

app = Sanic()


@app.route('/')
@app.route('/<object_id>')
@app.route('/<object_id>/<section>')
async def test(request, object_id='', section=None):
    return json({'object_id': object_id,
                 'section': section})
