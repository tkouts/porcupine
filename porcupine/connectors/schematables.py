from pypika import Table
from pypika.functions import Cast
from porcupine.core.schemaregistry import get_datatype_from_attr_name


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
        if name == 'content_class':
            name = 'type'
        if name in self.columns:
            # return column
            return super().field(name)
        else:
            if '.' in name:
                attr, *path = name.split('.')
                dt = get_datatype_from_attr_name(
                    self.collection.accepts, attr
                )
                full_path = ','.join([dt.storage_key, *path])
                return self.data_field.get_path_text_value(f'{{{full_path}}}')
            else:
                dt = get_datatype_from_attr_name(
                    self.collection.accepts, name
                )
                return Cast(
                    self.data_field.get_text_value(dt.storage_key),
                    dt.db_cast_type
                ).as_(name)


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
        'id', 'sig', 'type', 'parent_id', 'p_type'
    )
    # partial_fields = (
    #     'id', 'parent_id', 'type', 'acl', 'is_system',
    #     'is_deleted', 'expires_at'
    # )
    # table_name = 'composites'
