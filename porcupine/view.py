class view:
    def __init__(self, get=None, post=None):
        self.get = get
        self.post = post

    def http_post(self, handler):
        return type(self)(get=self.get, post=handler)
