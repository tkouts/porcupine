import sys
import pkgutil


def import_all(path):
    for loader, name, is_pkg in pkgutil.walk_packages([path]):
        if name not in sys.modules:
            loader.find_module(name).load_module(name)
