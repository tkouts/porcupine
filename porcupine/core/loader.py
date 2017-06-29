import sys
import pkgutil
import inspect
from .server import server
from .app import App
from .log import porcupine_log


def install_apps(path: list, prefix: str='') -> None:
    apps = {}

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
                apps[value.name] = value

    # install apps
    for app_name, app in apps.items():
        porcupine_log.info(
            'Installing application {0}'.format(app_name))
        server.blueprint(app, url_prefix=app_name)
