# import msgpack
import cbor
from porcupine.utils import system
from porcupine.core.abstract.session import AbstractSessionManager


class SessionManager(AbstractSessionManager):
    def __init__(self, **params):
        super().__init__(**params)
        self.secret = self.params['secret'].encode('utf-8')

    def generate_sig(self, session):
        return system.hash_series(
            session.id,
            session.user_id,
            self.secret,
            using='sha3_256').hexdigest()

    async def load(self, request):
        session = None
        i = 0
        chunks = []
        cookie = request.cookies.get('_s{0}'.format(i))
        while cookie is not None:
            chunks.append(cookie)
            i += 1
            cookie = request.cookies.get('_s{0}'.format(i))
        if chunks:
            session_bytes = ''.join(chunks).encode('latin-1')
            session = self.SessionType(
                # msgpack.loads(session_bytes, encoding='utf-8')
                cbor.loads(session_bytes)
            )
            sig = self.generate_sig(session)
            if session['sig'] != sig:
                session = None
        return session

    async def save(self, request, response):
        session = request['session']
        # update signature
        session['sig'] = self.generate_sig(session)
        # chunk = msgpack.dumps(request['session'],
        #                       use_bin_type=True).decode('latin-1')
        chunk = cbor.dumps(session).decode('latin-1')
        chunks = [chunk[i:i + 4000]
                  for i in range(0, len(chunk), 4000)]
        for i in range(len(chunks)):
            cookie_name = '_s{0}'.format(i)
            response.cookies[cookie_name] = chunks[i]
            response.cookies[cookie_name]['httponly'] = True
        # remove extra cookies
        await self.remove(request, response, len(chunks))

    async def remove(self, request, response, start=0):
        j = 0
        next_cookie = request.cookies.get('_s{0}'.format(j))
        while next_cookie is not None:
            del response.cookies['_s{0}'.format(j)]
            j += 1
            next_cookie = request.cookies.get('_s{0}'.format(j))
