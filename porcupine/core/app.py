import sys
import os

from sanic import Blueprint

from porcupine import db, config
from porcupine.apps.schema.users import SystemUser
from porcupine.core import utils
from porcupine.db import transactional
from .context import with_context, system_override


class App(Blueprint):
    name = None
    db_blueprint = None

    def __init__(self):
        super().__init__(self.name)

    async def setup_db_blueprint(self):
        if self.db_blueprint is not None:
            app_class_dir = os.path.abspath(
                os.path.dirname(
                    sys.modules[type(self).__module__].__file__))
            blueprint_file = os.path.join(app_class_dir, self.db_blueprint)
            db_blueprint = config.parse(blueprint_file)
            await self.__initialize_db(db_blueprint)

    @with_context(SystemUser())
    @transactional()
    async def __initialize_db(self, blueprint):
        for item in blueprint:
            await self.__process_item(item, None)

    async def __process_item(self, item_dict, parent):
        item_id = item_dict.pop('id', None)
        item_type = item_dict.pop('type', None)
        children = item_dict.pop('children', [])

        # resolve item
        if item_id:
            if parent:
                item = await parent.get_child_by_id(item_id)
            else:
                item = await db.connector.get(item_id)
        elif parent:
            item = await parent.get_child_by_name(item_dict['name'])
        else:
            item = None

        if item is None:
            item = utils.get_content_class(item_type)()
            if item_id:
                # restore id in dict so it is set
                item_dict['id'] = item_id

        if item_dict:
            with system_override():
                await item.apply_patch(item_dict)

            if item.__is_new__:
                await item.append_to(parent)
            else:
                await item.update()

        for child_dict in children:
            await self.__process_item(child_dict, item)
