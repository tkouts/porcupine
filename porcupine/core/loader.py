import sys
import pkgutil
import inspect
from .app import App
from .log import porcupine_log


def gather_apps(server, path: list, prefix: str='') -> None:
    apps = []

    # locate apps in path
    for loader, name, is_pkg in pkgutil.walk_packages(path, prefix=prefix):
        if name not in sys.modules:
            mod = loader.find_module(name).load_module(name)
        else:
            mod = sys.modules[name]

        for member_name, value in inspect.getmembers(mod):
            if member_name.startswith('__'):
                continue
            if isinstance(value, App) and sys.modules[value.__module__] == mod:
                apps.append(value)

    # install apps
    for app in apps:
        porcupine_log.info('Installing application {0}'.format(app.name))
        server.blueprint(app, url_prefix=app.name)


def import_all(path):
    for loader, name, is_pkg in pkgutil.walk_packages([path]):
        if name not in sys.modules:
            loader.find_module(name).load_module(name)
