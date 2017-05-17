"""
Session Manager Service
"""
import asyncio

from sanic import Blueprint

from porcupine import log
from porcupine.config import settings
from porcupine.core.utils import system
from .service import AbstractService

session_manager = None
session_manager_bp = Blueprint('session_manager')


@session_manager_bp.middleware('request')
async def add_session_to_request(request):
    session = await session_manager.load(request)
    if session is None:
        session = session_manager.new_session()
    # print('session is', session)
    request['session'] = session


@session_manager_bp.middleware('response')
async def save_session(request, response):
    session = request['session']
    if session.is_terminated:
        _ = session_manager.remove(request, response)
        if asyncio.iscoroutine(_):
            await _
    elif session.is_dirty:
        # print('saving session')
        await session_manager.save(request, response)


class SessionManager(AbstractService):
    @classmethod
    async def start(cls, server):
        global session_manager
        log.info('Creating session manager')
        sm_type = system.get_rto_by_name(settings['session_manager']['type'])
        session_manager = sm_type(**settings['session_manager']['params'])
        await session_manager.initialize()
        # register middleware
        server.blueprint(session_manager_bp)

    @classmethod
    async def stop(cls, server):
        log.info('Closing session manager')
        await session_manager.close()
