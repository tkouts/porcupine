from sanic import Blueprint


class App(Blueprint):

    @classmethod
    def install(cls):
        from porcupine.bootstrap import sanic
        app = cls()
        sanic.blueprint(app)

    def __init__(self, name):
        super().__init__(name)
        self.listeners['before_server_start'].append(self.before_start)
        self.listeners['after_server_start'].append(self.after_start)
        self.listeners['before_server_stop'].append(self.before_stop)
        self.listeners['after_server_stop'].append(self.after_stop)

    @staticmethod
    def before_start(app, loop):
        pass

    @staticmethod
    def after_start(app, loop):
        pass

    @staticmethod
    def before_stop(app, loop):
        pass

    @staticmethod
    def after_stop(app, loop):
        pass