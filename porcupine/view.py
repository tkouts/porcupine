class view:
    def __init__(self, get=None, post=None, put=None):
        self.get = get
        self.post = post
        self.put = put

    def http_post(self, handler):
        return type(self)(get=self.get, post=handler, put=self.put)

    def http_put(self, handler):
        return type(self)(get=self.get, post=self.post, put=handler)
