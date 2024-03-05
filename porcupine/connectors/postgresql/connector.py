# from typing import Optional
from collections import OrderedDict

import asyncpg
from lru import LRU

from porcupine import log, exceptions
from .transaction import Transaction
from porcupine.connectors.schematables import ItemsTable
from porcupine.core.accesscontroller import resolve_visibility
from porcupine.core.utils import hash_series
# from porcupine.core.stream.streamer import BaseStreamer
# from porcupine.core.schema.partial import PartialItem
from .query import PorcupineQuery, QueryType
from pypika import Query
from porcupine.connectors.postgresql import persist
from porcupine.core import schemaregistry

# from pypika import Table


class Postgresql:
    def __init__(self, server):
        self.server = server
        self.txn_max_retries = int(server.config.DB_TXN_MAX_RETRIES)
        self.cache_size = int(server.config.DB_CACHE_SIZE)
        self.pool = None
        self.active_txns = 0

    def config(self):
        return {
            'cache_size': self.cache_size,
            'txn_max_retries': self.txn_max_retries
        }

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            f'{self.server.config.DB_HOST}/{self.server.config.DB_NAME}',
            user=self.server.config.DB_USER,
            password=self.server.config.DB_PASSWORD
        )

    def acquire(self):
        return PoolAcquire(self)

    async def prepare_indexes(self):
        log.info('Preparing indexes...')
        async with self.pool.acquire() as db:
            await db.execute('''
                create table if not exists items (
                    id text primary key not null,
                    sig text not null,
                    type text not null,
                    acl jsonb,
                    name text not null,
                    created text not null,
                    modified text not null,
                    is_collection boolean,
                    is_system boolean,
                    parent_id text REFERENCES items(id) ON DELETE CASCADE,
                    p_type text,
                    expires_at integer,
                    is_deleted integer,
                    data jsonb not null
                )
            ''')

            ##############
            # compositions
            ##############
            for cls, composition in schemaregistry.get_compositions():
                table = composition.t
                await db.execute(f'''
                    create table if not exists {table.get_table_name()} (
                        id text primary key not null,
                        sig text not null,
                        type text not null,
                        parent_id TEXT NOT NULL
                            REFERENCES {cls.table_name()}(id) ON DELETE CASCADE,
                        p_type text NOT NULL,
                        data jsonb not null
                    )
                ''')

            ####################
            # indexes
            ####################
            for index, index_info in schemaregistry.get_indexes().items():
                dt = index_info['dt']
                subclasses = index_info['cls']
                # print(dt.t.get_table_name(), index.on, subclasses)
                quoted_subclasses = [f"'{s.__name__}'" for s in subclasses]
                db_fields = [getattr(dt.t, attr) for attr in index.on]
                prefix = 'UX' if index.unique else 'IX'
                index_name = (
                    f'{prefix}'
                    f'_{dt.name}'
                    f'_{hash_series(quoted_subclasses)[:8]}'
                    f'_{"_".join(index.on)}'
                )
                extra = ''
                if index.when_value_is:
                    extra_conditions = [
                        str(f == v)
                        for f, v in zip(db_fields, index.when_value_is)
                    ]
                    extra = f' and {" and ".join(extra_conditions)}'
                    # print(extra)
                # print(f'''
                #     create{' unique' if index.unique else ''} index
                #     if not exists {index_name}
                #     on {dt.t.get_table_name()}
                #     ("parent_id", {', '.join([str(f) for f in db_fields])})
                #     where p_type in ({", ".join(quoted_subclasses)}){extra};
                # ''')
                await db.execute(f'''
                    create{' unique' if index.unique else ''} index
                    if not exists {index_name}
                    on {dt.t.get_table_name()}
                    ("parent_id", {', '.join([str(f) for f in db_fields])})
                    where p_type in ({", ".join(quoted_subclasses)}){extra};
                ''')
                # print(dt.name, hash_series(subclasses))

            ########################
            # many-to-many relations
            ########################
            many_to_many = {
                d.associative_table: d.associative_table_fields
                for d in schemaregistry.get_many_to_many_relationships()
            }
            print(many_to_many)
            for table, fields in many_to_many.items():
                await db.execute(f'''
                    create table if not exists {table.get_table_name()} (
                        {fields[0]} text not null
                            REFERENCES items(id) ON DELETE CASCADE,
                        {fields[1]} text not null
                            REFERENCES items(id) ON DELETE CASCADE,
                        UNIQUE ({",".join(fields)})
                    )
                ''')

            ###################
            # full text indexes
            ###################
            fts_indexes = schemaregistry.get_fts_indexes()
            for cls, indexed_attributes, subclasses in fts_indexes:
                print(cls, indexed_attributes, subclasses)
                quoted_subclasses = [f"'{s}'" for s in subclasses]
                items_table = ItemsTable(cls.children)
                schema_attributes = [
                    str(getattr(items_table, a))
                    for a in indexed_attributes
                ]
                for attr_name, attr in zip(indexed_attributes,
                                           schema_attributes):
                    index_name = (
                        f'FTS'
                        f'_{cls.__name__.lower()}'
                        f'_{hash_series(subclasses)[:8]}'
                        f'_{attr_name.replace(".", "_")}'
                    )
                    await db.execute(
                        f'CREATE INDEX IF NOT EXISTS {index_name}'
                        ' ON items '
                        f'USING GIN (to_tsvector(\'english\', {attr})) '
                        f'where p_type in ({",".join(quoted_subclasses)});'
                    )


class PoolAcquire:
    def __init__(self, connector):
        self.connector = connector
        self.connection = None

    async def __aenter__(self):
        self.connection = await self.connector.pool.acquire()
        return Connection(self.connection, self.connector.cache_size)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # print('RELEASING')
        await self.connector.pool.release(self.connection)
        self.connection = None


class Connection:
    persist = persist
    supports_ttl = False
    Query = PorcupineQuery
    __slots__ = 'db', 'cache', 'txn'

    def __init__(self, connection, cache_size):
        self.db = connection
        self.cache = LRU(cache_size)
        self.txn = None

    def get_transaction(self):
        if self.txn is None:
            self.txn = Transaction(self)
        return self.txn

    async def get(self, object_id, quiet=True, _table='items'):
        if self.txn is not None and object_id in self.txn:
            return self.txn[object_id]
        item = self.cache.get(object_id, None)
        if item is None:
            # print('getting', object_id)
            item = await self.get_raw(object_id, _table)
            if item is not None:
                item = self.persist.loads(item)
                is_visible = await resolve_visibility(item)
                if not is_visible:
                    item = None
            self.cache[object_id] = item
        if not quiet and item is None:
            raise exceptions.NotFound(
                f'The resource {object_id} does not exist.'
            )
        return item

    async def get_raw(self, item_id, table):
        result = await self.db.fetchrow(
            f'select * from "{table}" where id=$1',
            item_id
        )
        return result

    async def get_multi(self, item_ids, quiet=True):
        t = ItemsTable(None)
        ordered_ids = OrderedDict()

        for oid in item_ids:
            if self.txn is not None and oid in self.txn:
                ordered_ids[oid] = self.txn[oid]
            else:
                ordered_ids[oid] = self.cache.get(oid, False)

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
                self.cache[oid] = item
                if item is not None:
                    yield item

    def fetch_access_map(self, item_id):
        return self.query('''
            with recursive
                parent_ids(id) as (
                    values($1)
                    UNION
                    select items.parent_id from items, parent_ids
                    where items.id=parent_ids.id
                )
            select id, parent_id, acl, is_deleted, expires_at
            from items where items.id in (select id from parent_ids);
        ''', [item_id])

    # def get_cursor(self, query, **params):
    #     return Cursor(self, query, **params)

    def query(self, query, params):
        # print(query, params)
        positional = params
        if isinstance(params, dict):
            # convert params to positional
            positional = []
            i = 1
            for param, value in params.items():
                query = query.replace(f':{param}', f'${i}')
                positional.append(value)
                i += 1
        return self.db.fetch(query, *positional)
