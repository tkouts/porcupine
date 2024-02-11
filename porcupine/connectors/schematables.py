from pypika import Table
from porcupine.core.utils import get_storage_key_from_attr_name


class SchemaTable(Table):
    columns = ()
    partial_fields = ()
    table_name = None

    def __init__(
        self,
        collection,
        name=None,
        schema=None,
        alias=None,
        query_cls=None,
    ):
        self.collection = collection
        super().__init__(name or self.table_name, schema, alias, query_cls)
        self.data_field = super().field('data')

    def field(self, name: str):
        if name in self.columns:
            return super().field(name)
        else:
            alias = None
            if '.' in name:
                attr, path = name.split('.', 1)
                storage_key = get_storage_key_from_attr_name(
                    self.collection.accepts, attr
                ) or attr
                full_path = '.'.join([storage_key, path])
            else:
                storage_key = get_storage_key_from_attr_name(
                    self.collection.accepts, name
                ) or name
                full_path = storage_key
                alias = name
            return self.data_field.get_text_value(full_path).as_(alias)


class ItemsTable(SchemaTable):
    columns = (
        'id', 'sig', 'type', 'acl', 'name', 'created',
        'modified', 'is_collection', 'is_system',
        'parent_id', 'p_type', 'expires_at', 'is_deleted'
    )
    partial_fields = (
        'id', 'parent_id', 'type', 'acl', 'is_system',
        'is_deleted', 'expires_at'
    )
    table_name = 'items'


class CompositesTable(SchemaTable):
    columns = (
        'id', 'sig', 'type', 'item_id'
    )
    # partial_fields = (
    #     'id', 'parent_id', 'type', 'acl', 'is_system',
    #     'is_deleted', 'expires_at'
    # )
    # table_name = 'composites'
