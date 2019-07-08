import multiprocessing
from sanic.response import json
from porcupine import App, __version__
from porcupine.core.services import get_service


class Status(App):
    name = 'status'


status = Status()


@status.route('/', methods=frozenset({'GET'}))
async def status_handler(_):
    process_name = multiprocessing.current_process().name
    services = get_service(None)
    services_status = {key: await service.status()
                       for (key, service) in services.items()}
    return json({
        'version': __version__,
        'process_name': process_name,
        'services': services_status
    })
