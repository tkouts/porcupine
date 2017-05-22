import sys

import os
import yaml
from sanic import Blueprint

from porcupine import db
from porcupine.apps.schema.users import SystemUser
from porcupine.core import utils
from porcupine.db import transactional
from .context import with_context, system_override


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
        for item in blueprint:
            await self.__process_item(item, None)

    async def __process_item(self, item_dict, parent):
        item_id = item_dict.pop('id', None)
        item_type = item_dict.pop('type')
        children = item_dict.pop('children', [])
        if item_id:
            item = await db.connector.get(item_id)
        else:
            item = None
        if item is None:
            item = utils.get_content_class(item_type)()
            if item_id:
                # restore id in dict so it is set
                item_dict['id'] = item_id

        with system_override():
            await item.apply_patch(item_dict)

        if item.__is_new__:
            await item.append_to(parent)
        else:
            await item.update()

        for child_dict in children:
            await self.__process_item(child_dict, item)
