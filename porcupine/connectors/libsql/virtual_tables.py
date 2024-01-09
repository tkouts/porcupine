from pypika import Table
from pypika.terms import Function

from porcupine.core.utils import get_storage_key_from_attr_name
from porcupine.core.schema.container import Container


class JsonExtract(Function):
    def __init__(self, *args, alias=None):
        super().__init__('json_extract', *args, alias=alias)


class VirtualTable(Table):
    item_columns = (
        'id', 'sig', 'type', 'name', 'created', 'modified', 'is_collection',
        'acl', 'parent_id', 'p_type', 'expires_at', 'deleted'
    )

    def __init__(
        self,
        collection,
        schema=None,
        alias=None,
        query_cls=None,
    ):
        self.collection = collection
        self.columns = VirtualTable.item_columns
        table_name = 'items'
        super().__init__(table_name, schema, alias, query_cls)
        self.data_field = super().field('data')

    def field(self, name: str):
        # print(name)
        if name in self.columns:
            return super().field(name)
        else:
            if '.' in name:
                attr, path = name.split('.', 1)
                storage_key = get_storage_key_from_attr_name(
                    self.collection._desc.accepts, attr
                ) or attr
                full_path = '.'.join([storage_key, path])
            else:
                storage_key = get_storage_key_from_attr_name(
                    self.collection._desc.accepts, name
                ) or name
                full_path = storage_key
            return JsonExtract(self.data_field, f'$.{full_path}')


# class ItemsVirtualTable(Table):
#     columns = (
#         'id', 'sig', 'type', 'name', 'created', 'modified', 'is_collection',
#         'acl', 'parent_id', 'p_type', 'expires_at', 'deleted'
#     )
#     def __init__(
#         self,
#         name: str,
#         schema=None,
#         alias=None,
#         query_cls=None,
#     ):
#         super().__init__(name, schema, alias, query_cls)
#         self.fields = 'id', 'name'
#         self.data_field = super().field('data')
