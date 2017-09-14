import abc

from porcupine.core import utils
from porcupine.core.utils.observables import ObservableDict


class Session(ObservableDict):
    def __init__(self, seq=(), **kwargs):
        super().__init__(seq, **kwargs)
        self._is_dirty = False
        self._is_removed = False

    @property
    def id(self):
        return self.get('id')

    @property
    def user_id(self):
        return self.get('uid')

    @property
    def is_dirty(self):
        return self._is_dirty

    @property
    def is_terminated(self):
        return self._is_removed

    def terminate(self):
        self._is_removed = True

    def on_after_mutate(self):
        self._is_dirty = True


class AbstractSessionManager(metaclass=abc.ABCMeta):
    revive_threshold = 60.0
    SessionType = Session

    def __init__(self, server):
        self.server = server
        self.timeout = int(server.config.SM_SESSION_TIMEOUT)
        self.guest_user_id = server.config.SM_GUEST_USER_ID

    async def initialize(self):
        pass

    def new_session(self):
        session = self.SessionType(id=utils.generate_oid(12))
        session['uid'] = self.guest_user_id
        return session

    @abc.abstractmethod
    async def load(self, request):
        raise NotImplementedError

    @abc.abstractmethod
    async def save(self, request, response):
        raise NotImplementedError

    @abc.abstractmethod
    async def remove(self, request, response):
        raise NotImplementedError

    async def close(self):
        pass
