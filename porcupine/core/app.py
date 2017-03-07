import os
import sys
import yaml
from sanic import Blueprint

from porcupine import db
from porcupine.utils import system
from porcupine.db import transactional
from .context import context, with_context, system_override


class App(Blueprint):
    db_blueprint = None

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

    async def before_start(self, app, loop):
        if self.db_blueprint is not None:
            await self.__initialize_db()

    def after_start(self, app, loop):
        pass

    def before_stop(self, app, loop):
        pass

    def after_stop(self, app, loop):
        pass

    @with_context
    @transactional()
    async def __initialize_db(self):
        from porcupine.schema.system.users import SystemUser
        context.user = SystemUser()
        app_class_dir = os.path.abspath(
            os.path.dirname(
                sys.modules[self.__class__.__module__].__file__))
        blueprint_file = os.path.join(app_class_dir, self.db_blueprint)
        with open(blueprint_file, encoding='utf-8') as f:
            db_blueprint = yaml.load(f.read())
        for item in db_blueprint.get('items', []):
            await self.__process_item(item, None)

    async def __process_item(self, item_dict, parent):
        # print(item_dict['type'], context.data)
        item_id = item_dict.pop('id')
        item = await db.get_item(item_id)
        if item is None:
            item = system.get_rto_by_name(item_dict.pop('type'))()
            item.id = item_id
        children = item_dict.pop('children', [])

        with system_override():
            for attr, value in item_dict.items():
                setattr(item, attr, value)

        if item.__is_new__:
            item.append_to(parent)

        for child_dict in children:
            await self.__process_item(child_dict, item)
        print(item, item.__snapshot__)
