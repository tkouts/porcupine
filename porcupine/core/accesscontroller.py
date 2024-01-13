import orjson
import time
from collections import namedtuple, ChainMap
from porcupine.core.services import db_connector
from porcupine.core.context import (
    ctx_access_map,
    ctx_user,
    ctx_sys,
    ctx_visibility_cache
)

AccessRecord = namedtuple(
    'AccessRecord',
    ['parent_id', 'acl', 'is_deleted', 'expires_at']
)


def _iter_access_item(parent_id, access_item):
    access_map = ctx_access_map.get()
    while parent_id is not None:
        access_record = access_map[parent_id]
        yield getattr(access_record, access_item)
        parent_id = access_record.parent_id


def _is_deleted(item):
    if item.is_deleted:
        return True
    parent_id = item.parent_id
    if parent_id is not None:
        for deleted in _iter_access_item(parent_id, 'is_deleted'):
            if deleted:
                return True
    return False


def _is_expired(item):
    # print('expired')
    parent_id = item.parent_id
    expiry_list = [
        e for e in _iter_access_item(parent_id, 'expires_at')
        if e is not None
    ]
    if item.expires_at is not None:
        expiry_list.append(item.expires_at)

    if expiry_list:
        return time.time() > min(expiry_list)

    return False


def resolve_acl(item):
    acl = item.acl.to_json()
    parent_id = item.parent_id
    if parent_id is None:
        return acl or {}
    if acl is not None and '__partial__' not in acl:
        return acl
    acls = []
    if acl is not None:
        acls.append(acl)
    parent_acls = _iter_access_item(parent_id, 'acl')
    for p_acl in parent_acls:
        if p_acl is not None:
            acls.append(p_acl)
            if '__partial__' not in p_acl:
                break
    return ChainMap(*acls)


async def resolve_visibility(item) -> bool:
    if item.__is_new__ or ctx_sys.get():
        return True

    connector = db_connector()

    if item.is_composite:
        return await resolve_visibility(await connector.get(item.item_id))

    access_map = ctx_access_map.get()
    parent_id = item.parent_id
    if parent_id is None:
        # ROOT container
        access_map[item.id] = item.access_record
    elif parent_id not in access_map:
        results = await connector.fetch_access_map(parent_id)
        access_map.update({
            row['id']: AccessRecord(row['parent_id'],
                                    row['acl'] and
                                    orjson.loads(row['acl']),
                                    row['is_deleted'],
                                    row['expires_at'])
            for row in results
            if row['id'] not in access_map
        })

    user = ctx_user.get()
    visibility_cache = ctx_visibility_cache.get()
    use_cache = (
        parent_id is not None
        and not item.is_deleted
        and item.expires_at is None
        and not item.acl.is_set()
    )
    cache_key = f'{parent_id}|{user.id if user else ""}'

    if use_cache and cache_key in visibility_cache:
        # print('using cache', item)
        return visibility_cache[cache_key]

    can_read = True

    if not item.is_system:
        # check recycled / expired
        deleted = _is_deleted(item)
        if deleted:
            can_read = False
        elif not connector.supports_ttl:
            expired = _is_expired(item)
            if expired:
                can_read = False
    can_read = can_read and await item.can_read(user)

    if use_cache:
        visibility_cache[cache_key] = can_read

    return can_read
