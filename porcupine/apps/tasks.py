import time

from porcupine import server, db
from porcupine.core.context import system_override
from porcupine.connectors.postgresql.query import PorcupineQuery
from porcupine.connectors.schematables import ItemsTable
from pypika import Parameter


@db.transactional()
async def remove_expired_item(item):
    await item.remove()


@server.cron_tab('@hourly')
async def delete_expired_items():
    """Removes expired items"""
    print('running delete_expired_items')
    now = time.time()
    t = ItemsTable(None)
    q = (
        PorcupineQuery
        .from_(t)
        .select(t.star)
        .where(t.expires_at <= Parameter(':now'))
    )
    with system_override():
        async for item in q.cursor(now=now):
            # print('expired', item)
            await remove_expired_item(item)
