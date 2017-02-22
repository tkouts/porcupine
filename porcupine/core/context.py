from .aiolocals.local import Local


class Context(Local):
    def init(self):
        setattr(self, 'user', None)
        setattr(self, 'data', {})
        setattr(self, 'txn', None)
