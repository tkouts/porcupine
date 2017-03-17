from sanic.response import json

from porcupine import context, exceptions, db, server
from porcupine.contract import is_new_item
from porcupine.utils import permissions, system
from .mutable import Dictionary
from .common import String
from .reference import ReferenceN


class Acl(Dictionary):
    async def on_change(self, instance, value, old_value):
        acl = await instance.applied_acl
        user_role = await permissions.resolve(acl, context.user)
        if user_role < permissions.COORDINATOR:
            raise exceptions.Forbidden(
                'The user does not have permissions '
                'to modify the access control list.')
        super().on_change(instance, value, old_value)


class SchemaSignature(String):
    readonly = True
    protected = True


class Children(ReferenceN):
    readonly = True

    def __get__(self, instance, owner):
        if instance is None:
            # create a separate instance per owner
            # with accepting the container's allowed types
            if self.name not in owner.__dict__:
                children = Children(default=self._default,
                                    accepts=owner.containment)
                setattr(owner, 'children', children)
                return children
            return self
        return super().__get__(instance, owner)

    async def get(self, request, instance, expand=True):
        return await super().get(request, instance, True)

    @is_new_item()
    @db.transactional()
    async def post(self, request, instance):
        item_dict = request.json
        # TODO: handle invalid type exception
        item_type = system.get_rto_by_name(item_dict.pop('type'))
        new_item = item_type()
        for attr, value in item_dict.items():
            setattr(new_item, attr, value)
        await new_item.append_to(instance)
        location = server.url_for('resources.resource_handler',
                                  item_id=new_item.id)
        return json(
            new_item.id,
            status=201,
            headers={
                'Location': location
            })
