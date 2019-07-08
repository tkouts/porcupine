"""
App installer service
"""
import os
import sys
import pkgutil
import inspect
from multiprocessing import Lock

from porcupine import apps
from porcupine.core.app import App
from porcupine.core.log import porcupine_log
from .service import AbstractService


class AppInstaller(AbstractService):
    service_key = 'app_installer'
    _DB_BP_LOCK = Lock()

    def __init__(self, server):
        super().__init__(server)
        self.apps = []
        # gather native apps
        scan_dir = os.getcwd()
        self.gather_apps(apps.__path__, prefix='porcupine.apps.')
        porcupine_path = os.path.dirname(
            os.path.dirname(sys.modules['porcupine'].__file__))
        if not scan_dir.startswith(porcupine_path):
            # gather user apps
            os.chdir(scan_dir)
            self.gather_apps([scan_dir])
            # check if there is a static directory
            static_dir_path = os.path.join(scan_dir, 'static')
            if os.path.isdir(static_dir_path):
                self.server.static('/', static_dir_path)

    def gather_apps(self, path: list, prefix: str = '') -> None:
        found_apps = []
        # locate apps in path
        for loader, name, is_pkg in pkgutil.walk_packages(path, prefix=prefix):
            if name not in sys.modules:
                mod = loader.find_module(name).load_module(name)
            else:
                mod = sys.modules[name]

            for member_name, value in inspect.getmembers(mod):
                if member_name.startswith('__'):
                    continue
                if isinstance(value, App) and \
                        sys.modules[value.__module__] == mod:
                    found_apps.append(value)
        # install apps
        for app in found_apps:
            porcupine_log.info('Installing application {0}'.format(app.name))
            self.apps.append(app)
            self.server.blueprint(app, url_prefix=app.name)

    async def start(self, loop):
        lock_acquired = self._DB_BP_LOCK.acquire(False)
        if lock_acquired:
            # install apps db blueprints
            for app in self.apps:
                await app.setup_db_blueprint()
            self._DB_BP_LOCK.release()

    def stop(self, loop):
        ...

    async def status(self):
        return {
            'apps': [app.name for app in self.apps]
        }
