"""
Porcupine database utilities
"""
import time
from typing import Optional

from porcupine.hinting import TYPING
from porcupine.core.context import context
from porcupine.core.services import db_connector, get_service


async def resolve_visibility(item: TYPING.ANY_ITEM_CO) -> Optional[bool]:
    if item is None:
        return None
    if context.system_override or item.__is_new__:
        return True

    # check for stale / expired / deleted
    use_cache = (
        not item.is_deleted
        and (item.is_composite or not item.acl.is_set())
    )
    user = context.user
    if item.is_composite:
        container = item.item_id
    else:
        container = item.parent_id
    cache_key = f'{container}|{user.id if user else ""}'

    if cache_key in context.visibility_cache:
        visibility = context.visibility_cache[cache_key]
        if visibility is None:
            return None
        if use_cache:
            return visibility

    it = item
    visibility = 0
    connector = db_connector()
    ttl_supported = connector.supports_ttl
    _get = connector.get
    while True:
        if not it.is_composite:
            # check expiration
            if not ttl_supported and it.expires_at is not None:
                if time.time() > it.expires_at:
                    # expired - remove from DB
                    await get_service('schema').remove_stale(it.id)
                    visibility = None
                    break
            if it.is_deleted:
                visibility = None
                break
            if it.is_system:
                break
        if it.parent_id is None:
            break
        else:
            parent = await _get(it.parent_id)
            if parent is None:
                # stale - remove from DB
                await get_service('schema').remove_stale(it.id)
                visibility = None
                break
            it = parent

    if visibility is None:
        if it != item:
            # add to perms cache
            context.visibility_cache[cache_key] = visibility
    else:
        visibility = await item.can_read(user)
        if use_cache:
            context.visibility_cache[cache_key] = visibility

    return visibility


def is_consistent(item, coll):
    return item is not None and coll.is_consistent(item)


async def get_with_id(item_id):
    item = await db_connector().get(item_id)
    return item_id, item
