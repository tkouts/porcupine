import sys
import os
import yaml

from sanic import Blueprint

from porcupine import db
from porcupine.db import transactional
from porcupine.utils import system
from .context import with_context, system_override
from porcupine.apps.schema.users import SystemUser


class App(Blueprint):
    name = None
    db_blueprint = None

    def __init__(self):
        super().__init__(self.name)
        self.listeners['before_server_start'].append(self.before_start)
        self.listeners['after_server_start'].append(self.after_start)
        self.listeners['before_server_stop'].append(self.before_stop)
        self.listeners['after_server_stop'].append(self.after_stop)

    async def before_start(self, app, loop):
        if self.db_blueprint is not None:
            app_class_dir = os.path.abspath(
                os.path.dirname(
                    sys.modules[self.__class__.__module__].__file__))
            blueprint_file = os.path.join(app_class_dir, self.db_blueprint)
            with open(blueprint_file, encoding='utf-8') as f:
                db_blueprint = yaml.load(f.read())
            await self.__initialize_db(db_blueprint)

    def after_start(self, app, loop):
        pass

    def before_stop(self, app, loop):
        pass

    def after_stop(self, app, loop):
        pass

    @with_context(SystemUser())
    @transactional()
    async def __initialize_db(self, blueprint):
        for item in blueprint.get('items', []):
            await self.__process_item(item, None)

    async def __process_item(self, item_dict, parent):
        item_id = item_dict.pop('id')
        item_type = item_dict.pop('type')
        children = item_dict.pop('children', [])
        item = await db.connector.get(item_id)
        if item is None:
            item = system.get_rto_by_name(item_type)()
            # restore id in dict so it is set
            item_dict['id'] = item_id

        with system_override():
            for attr, value in item_dict.items():
                setattr(item, attr, value)

        if item.__is_new__:
            await item.append_to(parent)
        else:
            await item.update()

        for child_dict in children:
            await self.__process_item(child_dict, item)
