import libsql_client
from porcupine import log, context, exceptions
from .transaction import Transaction
from porcupine.connectors.libsql import persist


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
        result = await self.db.execute(
            'select * from items where id=?',
            [item_id]
        )
        # print(item_id, len(result))
        if len(result) > 0:
            return result[0]

    async def query(self, query, params):
        result = await self.db.execute(query, params)
        return [self.persist.loads(row) for row in result]

    async def prepare_indexes(self):
        log.info('Preparing indexes...')
        await self.db.execute('''
            create table if not exists items (
                id text primary key not null,
                sig text not null,
                type text not null,
                name text not null,
                created text not null,
                modified text not null,
                is_collection boolean,
                acl json,
                parent_id text,
                p_type text,
                expires_at integer,
                deleted integer,
                data json not null
            )
        ''')
