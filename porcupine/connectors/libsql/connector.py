import libsql_client
from porcupine import log, context, exceptions
from .transaction import Transaction
# from .cursor import Cursor
# from .virtual_tables import ItemsTable
# from .query import PorcupineQuery
from porcupine.connectors.libsql import persist

# from pypika import Table


class LibSql:
    active_txns = 0
    persist = persist
    supports_ttl = False

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

    async def get(self, object_id, quiet=True, **kwargs):
        if context.txn is not None and object_id in context.txn:
            return context.txn[object_id]
        item = context.db_cache.get(object_id)
        if item is None:
            # print('getting', object_id)
            item = await self.get_raw(object_id)
            if item is not None:
                item = self.persist.loads(item)
            elif not quiet:
                raise exceptions.NotFound(
                    f'The resource {object_id} does not exist'
                )
            context.db_cache[object_id] = item
        return item

    async def get_raw(self, item_id):
        result = await self.query(
            'select * from items where id=?',
            [item_id]
        )
        if len(result) > 0:
            return result[0]

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
        await self.db.execute('''
            create index if not exists idx_is_collection on
            items(parent_id, is_collection)
        ''')
