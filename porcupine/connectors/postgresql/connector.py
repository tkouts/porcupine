# from typing import Optional
from collections import OrderedDict

import asyncpg
from lru import LRU

from porcupine import log, exceptions
from .transaction import Transaction
from porcupine.connectors.schematables import ItemsTable
from porcupine.core.accesscontroller import resolve_visibility
from porcupine.core.utils import hash_series
from .query import PorcupineQuery, QueryType
from porcupine.connectors.postgresql import persist
from porcupine.core import schemaregistry
from porcupine.db.index import Index


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

            # is_deleted function handler
            await db.execute('''
                CREATE OR REPLACE FUNCTION update_items_deleted()
                RETURNS TRIGGER LANGUAGE plpgsql AS 
                $$
                declare 
                  delta integer;
                begin
                  delta := NEW.is_deleted - OLD.is_deleted;
                  EXECUTE format(
                    'UPDATE %I SET is_deleted = %I.is_deleted + $1
                    WHERE %I.%I=$2',
                    TG_ARGV[0], TG_ARGV[0], TG_ARGV[0], TG_ARGV[1]
                  ) USING delta, NEW.id; 
                  RETURN NULL;
                end;
                $$;
            ''')
            await db.execute('''
                CREATE OR REPLACE TRIGGER updateItemsDeleted
                AFTER UPDATE OF is_deleted ON items
                FOR EACH ROW
                WHEN (NEW.is_collection)
                EXECUTE FUNCTION update_items_deleted('items', 'parent_id');
            ''')

            ##############
            # compositions
            ##############
            for cls, composition, subclasses in schemaregistry.get_compositions():
                table = composition.t
                table_name = table.get_table_name()
                reference_table = cls.table().get_table_name()
                await db.execute(f'''
                    create table if not exists {table_name} (
                        id text primary key not null,
                        sig text not null,
                        type text not null,
                        parent_id TEXT NOT NULL
                            REFERENCES {reference_table}(id)
                            ON DELETE CASCADE,
                        p_type text NOT NULL,
                        expires_at integer,
                        is_deleted integer,
                        data jsonb not null
                    )
                ''')
                if (
                    hasattr(composition, 'swappable')
                    and not composition.swappable
                ):
                    await db.execute(f'''
                        CREATE UNIQUE INDEX IF NOT EXISTS
                        UX_{composition.name}
                        ON {table_name}
                        ("parent_id")
                    ''')
                # add is_deleted trigger
                quoted_subclasses = tuple(
                    [f"'{s.__name__}'" for s in subclasses]
                )
                await db.execute(f'''
                    CREATE OR REPLACE TRIGGER
                    update{table_name.capitalize()}Deleted
                    AFTER UPDATE OF is_deleted ON {reference_table}
                    FOR EACH ROW
                    WHEN (NEW.type IN ({', '.join(quoted_subclasses)}))
                    EXECUTE FUNCTION update_items_deleted('{table_name}', 'parent_id');
                ''')

            ########################
            # many-to-many relations
            ########################
            many_to_many = {
                d.associative_table: d.associative_table_fields
                for d in schemaregistry.get_many_to_many_relationships()
            }
            # print(many_to_many)
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

            ####################
            # indexes
            ####################
            for index, index_info in schemaregistry.get_indexes().items():
                dt = index_info['dt']
                subclasses = index_info['cls']
                # print(dt.t.get_table_name(), index.on, subclasses)
                quoted_subclasses = tuple(
                    [f"'{s.__name__}'" for s in subclasses]
                )
                db_fields = tuple([str(f) for f in index.on])
                if isinstance(index, Index):
                    # Btree Index
                    prefix = 'UX' if index.unique else 'IX'
                    index_name = (
                        f'{prefix}'
                        f'_{dt.name}'
                        f'_{hash_series(quoted_subclasses + db_fields)[:8]}'
                    )
                    extra = ''
                    if index.where:
                        extra = f' AND {index.where}'
                    # print(f'''
                    #     CREATE{' UNIQUE' if index.unique else ''} INDEX
                    #     IF NOT EXISTS {index_name}
                    #     ON {dt.t.get_table_name()}
                    #     ("parent_id", {', '.join(db_fields)})
                    #     WHERE p_type IN ({', '.join(quoted_subclasses)}){extra};
                    # ''')
                    await db.execute(f'''
                        CREATE{' UNIQUE' if index.unique else ''} INDEX
                        IF NOT EXISTS {index_name}
                        ON {dt.t.get_table_name()}
                        ("parent_id", {', '.join(db_fields)})
                        WHERE p_type IN ({', '.join(quoted_subclasses)}){extra};
                    ''')
                else:
                    # FTS index
                    index_name = (
                        'FT'
                        f'_{dt.name}'
                        f'_{hash_series(quoted_subclasses + db_fields)[:8]}'
                    )
                    # print(f'''
                    #     CREATE INDEX IF NOT EXISTS {index_name}
                    #     ON {dt.t.get_table_name()}
                    #     USING GIN (to_tsvector(
                    #         '{index.locale}',
                    #         {" || ' ' || ".join(db_fields)}
                    #     ))
                    #     WHERE p_type IN ({', '.join(quoted_subclasses)});
                    # ''')
                    await db.execute(f'''
                        CREATE INDEX IF NOT EXISTS "{index_name}"
                        ON {dt.t.get_table_name()}
                        USING GIN (to_tsvector(
                            '{index.locale}',
                            {" || ' ' || ".join(db_fields)}
                        ))
                        WHERE p_type IN ({', '.join(quoted_subclasses)});
                    ''')


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
                is_visible = resolve_visibility(item)
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
            q = (
                self.Query
                .from_(t, query_type=QueryType.ITEMS)
                .select(t.star)
                .where(t.id.isin(fetch_from_db))
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
            select id, parent_id, acl
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
