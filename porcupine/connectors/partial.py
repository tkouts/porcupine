from typing import Mapping
import orjson
from porcupine.core.accesscontroller import Roles
from porcupine.core.utils.collections import OptionalFrozenDict
from porcupine.core.accesscontroller import resolve_acl, AccessRecord
from porcupine.core.context import ctx_sys
from porcupine.core.schemaregistry import get_content_class


class AclProxy(OptionalFrozenDict):
    def is_set(self) -> bool:
        return self._dct is not None


class PartialItem:
    __slots__ = '_partial', '_content_class', 'acl'

    def __init__(self, partial: Mapping):
        self._partial = partial
        self._content_class = get_content_class(partial['type'])
        if not self.is_composite:
            acl = partial['acl']
            self.acl = AclProxy(acl and orjson.loads(acl))

    @property
    def __is_new__(self):
        return False

    @property
    def access_record(self):
        return AccessRecord(
            self.parent_id,
            self.acl.to_json(),
            self.is_deleted,
            self.expires_at
        )

    @property
    def is_composite(self):
        return self._content_class.is_composite

    @property
    def is_collection(self):
        return self._content_class.is_collection

    @property
    def raw_data(self):
        return self._partial

    @property
    def content_class(self):
        return self._partial['type']

    @property
    def effective_acl(self):
        return resolve_acl(self)

    def __getitem__(self, item):
        return self._partial[item]

    def __getattr__(self, item):
        try:
            return self._partial[item]
        except KeyError:
            raise AttributeError(
                f"Partial[{self.content_class}]"
                f" object has no attribute '{item}'"
            )

    def __repr__(self) -> str:
        d = dict(self._partial)
        fields = [f'{k}={repr(v)}' for k, v in d.items()]
        return f"Partial[{self.content_class}]({' '.join(fields)})"

    def to_json(self):
        d = self._partial.asdict()
        remove = 'parent_id', 'acl'
        for k in remove:
            d.pop(k)
        return d

    async def can_read(self, membership):
        if self.is_composite:
            return True
        if ctx_sys.get():
            return True
        user_role = await Roles.resolve(self, membership)
        return user_role > Roles.NO_ACCESS
