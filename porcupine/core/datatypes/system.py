from sanic.response import json

from porcupine import context, exceptions, db, server
from porcupine.contract import is_new_item
from porcupine.utils import permissions, system
from .mutable import Dictionary
from .common import String
from .reference import ReferenceN


class Acl(Dictionary):
    allow_none = True

    async def on_change(self, instance, value, old_value):
        user_role = await permissions.resolve(instance, context.user)
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
    name = None

    def __get__(self, instance, owner):
        if instance is None:
            if self.name not in owner.__dict__:
                # create a separate instance per owner
                # accepting the container's containment types
                children = self.__class__(default=self._default,
                                          accepts=owner.containment)
                setattr(owner, self.__class__.name, children)
                return children
            return self
        return super().__get__(instance, owner)

    def accepts_item(self, item):
        if context.user.id == 'system':
            # allow for system
            return True
        return super().accepts_item(item)

    # HTTP views

    async def get(self, request, instance, expand=True):
        return await super().get(request, instance, True)

    @is_new_item()
    @db.transactional()
    async def post(self, request, instance):
        item_dict = request.json
        # TODO: handle invalid type exception
        item_type = system.get_rto_by_name(item_dict.pop('type'))
        new_item = item_type()
        try:
            for attr, value in item_dict.items():
                setattr(new_item, attr, value)
            await new_item.append_to(instance)
        except exceptions.AttributeSetError as e:
            raise exceptions.InvalidUsage(str(e))
        location = server.url_for('resources.resource_handler',
                                  item_id=new_item.id)
        return json(
            new_item.id,
            status=201,
            headers={
                'Location': location
            })


class Items(Children):
    name = 'items'

    @property
    def allowed_types(self):
        if not self.accepts_resolved:
            resolved = [
                system.get_rto_by_name(x) if isinstance(x, str) else x
                for x in self.accepts
            ]
            self.accepts = tuple([x for x in resolved if not x.is_collection])
        return self.accepts


class Containers(Children):
    name = 'containers'

    @property
    def allowed_types(self):
        if not self.accepts_resolved:
            resolved = [
                system.get_rto_by_name(x) if isinstance(x, str) else x
                for x in self.accepts
            ]
            self.accepts = tuple([x for x in resolved if x.is_collection])
        return self.accepts
