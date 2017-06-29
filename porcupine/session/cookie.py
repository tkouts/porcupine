import hashlib
import cbor
from porcupine.core.abstract.session import AbstractSessionManager


class SessionManager(AbstractSessionManager):
    def __init__(self, **params):
        super().__init__(**params)
        self.secret = self.params['secret'].encode('utf-8')

    def generate_sig(self, session):
        h = hashlib.blake2b(
            '{0}{1}'.format(session['id'], session['uid']).encode(),
            key=self.secret,
            digest_size=32
        )
        return h.hexdigest()

    async def load(self, request):
        session = None
        i = 0
        chunks = []
        cookie = request.cookies.get('_s{0}'.format(i))
        while cookie:
            chunks.append(cookie)
            i += 1
            cookie = request.cookies.get('_s{0}'.format(i))
        if chunks:
            session_bytes = ''.join(chunks).encode('latin-1')
            session = self.SessionType(
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
        chunk = cbor.dumps(session).decode('latin-1')
        chunks = [chunk[i:i + 4000]
                  for i in range(0, len(chunk), 4000)]
        for i in range(len(chunks)):
            cookie_name = '_s{0}'.format(i)
            response.cookies[cookie_name] = chunks[i]
            response.cookies[cookie_name]['httponly'] = True
        # remove extra cookies
        self.remove(request, response, len(chunks))

    def remove(self, request, response, start=0):
        j = 0
        next_cookie = request.cookies.get('_s{0}'.format(j))
        while next_cookie is not None:
            del response.cookies['_s{0}'.format(j)]
            j += 1
            next_cookie = request.cookies.get('_s{0}'.format(j))
