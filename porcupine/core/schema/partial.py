from typing import Mapping
import orjson
from porcupine.core.accesscontroller import Roles
from porcupine.core.utils.collections import OptionalFrozenDict
from porcupine.core.accesscontroller import resolve_acl
from porcupine.core.context import ctx_sys
from porcupine.core.utils import get_content_class


class AclProxy(OptionalFrozenDict):
    def is_set(self) -> bool:
        return self._dct is not None


class PartialItem:
    __slots__ = '_partial', '_content_class', 'acl'

    def __init__(self, partial=Mapping):
        self._partial = partial
        self._content_class = get_content_class(partial['type'])
        self.acl = AclProxy(partial['acl'] and orjson.loads(partial['acl']))

    @property
    def __is_new__(self):
        return False

    @property
    def is_composite(self):
        return self._content_class.is_composite

    @property
    def is_collection(self):
        return self._content_class.is_collection

    @property
    def content_class(self):
        return self._partial['type']

    @property
    def effective_acl(self):
        return resolve_acl(self)

    def __getattr__(self, item):
        try:
            return self._partial[item]
        except KeyError:
            raise AttributeError(
                f"Partial[{self.content_class}]"
                f" object has no attribute '{item}'"
            )

    def upgrade(self):
        row = self._partial
        content_class = self._content_class
        storage = orjson.loads(row['data'])
        storage['id'] = row['id']
        storage['sig'] = row['sig']
        if not content_class.is_composite:
            storage['acl'] = self.acl.to_json()
            storage['name'] = row['name']
            storage['cr'] = row['created']
            storage['md'] = row['modified']
            # params['is_collection'] = obj.is_collection
            storage['sys'] = row['is_system']
            storage['pid'] = row['parent_id']
            # params['p_type'] = dct.pop('_pcc', None)
            storage['exp'] = row['expires_at']
            storage['dl'] = row['is_deleted']
        return content_class(storage)

    async def can_read(self, membership):
        if ctx_sys.get():
            return True
        user_role = await Roles.resolve(self, membership)
        return user_role > Roles.NO_ACCESS
