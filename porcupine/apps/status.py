import multiprocessing
from sanic.response import json
from porcupine import App
from porcupine.core.services.blueprint import services


class Status(App):
    name = 'status'

status = Status()


@status.route('/', methods=frozenset({'GET'}))
async def status_handler(request):
    process_name = multiprocessing.current_process().name
    services_status = {service.__name__: service.status()
                       for service in services}
    return json({
        'process_name': process_name,
        'services': services_status
    })
