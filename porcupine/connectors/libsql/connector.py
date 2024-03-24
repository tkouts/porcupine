# from typing import Optional
from collections import OrderedDict

import libsql_client

from porcupine import log, context, exceptions
from .transaction import Transaction
from porcupine.connectors.schematables import ItemsTable
from porcupine.core.accesscontroller import resolve_visibility
# from porcupine.core.stream.streamer import BaseStreamer
# from porcupine.core.schema.partial import PartialItem
from .query import PorcupineQuery, QueryType
from pypika import Query
from porcupine.connectors.libsql import persist
from porcupine.core import schemaregistry

# from pypika import Table


class LibSql:
    active_txns = 0
    persist = persist
    supports_ttl = False
    Query = PorcupineQuery

    def __init__(self, server):
        self.server = server
        self.txn_max_retries = int(server.config.DB_TXN_MAX_RETRIES)
        self.cache_size = int(server.config.DB_CACHE_SIZE)
        self.db = None

    def get_transaction(self):
        return Transaction(self)

    async def connect(self):
        self.db = libsql_client.create_client(
            self.server.config.DB_HOST
        )

    async def get(self, object_id, quiet=True, _table='items'):
        if context.txn is not None and object_id in context.txn:
            return context.txn[object_id]
        item = context.db_cache.get(object_id)
        if item is None:
            # print('getting', object_id)
            item = await self.get_raw(object_id, _table)
            if item is not None:
                item = self.persist.loads(item)
                is_visible = resolve_visibility(item)
                if not is_visible:
                    item = None
            context.db_cache[object_id] = item
        if not quiet and item is None:
            raise exceptions.NotFound(
                f'The resource {object_id} does not exist.'
            )
        return item

    async def get_raw(self, item_id, table):
        result = await self.query(
            f'select * from "{table}" where id=?',
            [item_id]
        )
        if len(result) > 0:
            return result[0]

    async def get_multi(self, item_ids, quiet=True):
        t = ItemsTable(None)
        ordered_ids = OrderedDict()

        for oid in item_ids:
            if context.txn is not None and oid in context.txn:
                ordered_ids[oid] = context.txn[oid]
            else:
                ordered_ids[oid] = context.db_cache.get(oid, False)

        fetch_from_db = [
            oid for oid in ordered_ids
            if ordered_ids[oid] is False
        ]

        if fetch_from_db:
            q = self.Query(
                Query
                .from_(t)
                .select(t.star)
                .where(t.id.isin(fetch_from_db)),
                QueryType.ITEMS
            )
            async for item in q.cursor(_skip_acl_check=True):
                ordered_ids[item.id] = item

        for oid, item in ordered_ids.items():
            if not quiet and not item:
                raise exceptions.NotFound(
                    f'The resource {oid} does not exist.'
                )
            if item is not False:
                context.db_cache[oid] = item
                if item is not None:
                    yield item

    def fetch_access_map(self, item_id):
        return self.query('''
            with recursive
                parent_ids(id) as (
                    values(?)
                    UNION
                    select items.parent_id from items, parent_ids
                    where items.id=parent_ids.id
                )
            select id, parent_id, acl, is_deleted, expires_at
            from items where items.id in parent_ids;
        ''', [item_id])

    # def get_cursor(self, query, **params):
    #     return Cursor(self, query, **params)

    def query(self, query, params):
        # print(query, params)
        return self.db.execute(query, params)

    async def prepare_indexes(self):
        log.info('Preparing indexes...')
        await self.db.execute('''
            create table if not exists items (
                id text primary key not null,
                sig text not null,
                type text not null,
                acl json,
                name text not null,
                created text not null,
                modified text not null,
                is_collection boolean,
                is_system boolean,
                parent_id text REFERENCES items(id) ON DELETE CASCADE,
                p_type text,
                expires_at integer,
                is_deleted integer,
                data json not null
            )
        ''')
        # TODO: remove when indexes are implemented
        await self.db.execute('''
            create index if not exists idx_is_collection on
            items(parent_id, is_collection)
        ''')

        # many-to-many relations
        many_to_many = {
            d.associative_table: d.associative_table_fields
            for d in schemaregistry.get_many_to_many_relationships()
        }
        print(many_to_many)
        for table, fields in many_to_many.items():
            await self.db.execute(f'''
                create table if not exists "{table.get_table_name()}" (
                    {fields[0]} text not null
                        REFERENCES items(id) ON DELETE CASCADE,
                    {fields[1]} text not null
                        REFERENCES items(id) ON DELETE CASCADE,
                    UNIQUE ({",".join(fields)})
                )
            ''')

        # compositions
        for cls, composition in schemaregistry.get_compositions():
            await self.db.execute(f'''
                create table if not exists "{composition.t.get_table_name()}" (
                    id text primary key not null,
                    sig text not null,
                    type text not null,
                    item_id TEXT NOT NULL
                        REFERENCES {cls.table().get_table_name()}(id)
                        ON DELETE CASCADE,
                    data json not null
                )
            ''')

        # full text indexes
        fts_indexes = schemaregistry.get_fts_indexes()

        def get_fts_trigger_select(prefix, attrs, p_types, fts_oper=''):
            selects = []
            if fts_oper:
                selects.append(f"'{fts_oper}'")
            selects.append(f'{prefix}.ROWID')
            for schema_attr, alias in attrs:
                selects.append(f'{prefix}.{schema_attr} {alias}')
            return (
                f'SELECT {",".join(selects)} '
                f'WHERE {prefix}."p_type" in ({",".join(p_types)});'
            )

        for cls, indexed_attributes, subclasses in fts_indexes:
            print(cls, indexed_attributes, subclasses)
            fts_table_name = f'{cls.__name__}_fts'
            quoted_indexed_attributes = [f'"{a}"' for a in indexed_attributes]
            quoted_subclasses = [f"'{s}'" for s in subclasses]
            # TODO: add contentless_delete=1 option once libsql is upgraded
            items_table = ItemsTable(cls.children)
            await self.db.execute(f'''
                create virtual table if not exists "{fts_table_name}"
                using fts5(
                    {','.join(quoted_indexed_attributes)},
                    content='',
                    tokenize=porter
                )
            ''')
            # add triggers for keeping fts table in sync
            schema_attributes = [
                str(getattr(items_table, a))
                for a in indexed_attributes
            ]
            insert_select = get_fts_trigger_select(
                'new',
                zip(schema_attributes, quoted_indexed_attributes),
                quoted_subclasses
            )
            delete_select = get_fts_trigger_select(
                'old',
                zip(schema_attributes, quoted_indexed_attributes),
                quoted_subclasses,
                'delete'
            )
            await self.db.execute(f'''
                CREATE TRIGGER IF NOT EXISTS {fts_table_name}_sync_AFTER_INSERT
                AFTER INSERT ON items BEGIN
                  INSERT INTO {fts_table_name}(
                    rowid, {','.join(quoted_indexed_attributes)}
                  )
                  {insert_select}
                END
            ''')
            await self.db.execute(f'''
                CREATE TRIGGER IF NOT EXISTS {fts_table_name}_sync_AFTER_DELETE
                AFTER DELETE ON items BEGIN
                  INSERT INTO {fts_table_name}(
                    {fts_table_name}, rowid,
                    {','.join(quoted_indexed_attributes)}
                  )
                  {delete_select}
                END
            ''')
            await self.db.execute(f'''
                CREATE TRIGGER IF NOT EXISTS {fts_table_name}_sync_AFTER_UPDATE
                AFTER UPDATE ON items BEGIN
                  INSERT INTO {fts_table_name}(
                    {fts_table_name}, rowid,
                    {','.join(quoted_indexed_attributes)}
                  )
                  {delete_select}
                  INSERT INTO {fts_table_name}(
                    rowid, {','.join(quoted_indexed_attributes)}
                  )
                  {insert_select}
                END
            ''')
            # TODO: populate FTS index
            # for attr in indexed_attributes:
            #     print(items_table.field(attr).as_(attr).get_sql())

