import orjson
import time
from collections import namedtuple, ChainMap
from porcupine.core.services import db_connector
# from porcupine.core.schema.partial import PartialItem
# from porcupine import db
from porcupine.core.context import (
    ctx_access_map,
    # ctx_user,
    ctx_sys,
    ctx_visibility_cache,
    context_cacheable,
    # context_user
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
    return ChainMap(*acls) if len(acls) > 1 else acls[0]


async def resolve_visibility(item) -> bool:
    if item.__is_new__ or ctx_sys.get():
        return True

    connector = db_connector()

    if item.is_composite:
        return await resolve_visibility(await connector.get(item.item_id))

    parent_id = item.parent_id

    # check cache
    # user = ctx_user.get()
    visibility_cache = ctx_visibility_cache.get()
    use_cache = (
        parent_id is not None
        and not item.is_deleted
        and item.expires_at is None
        # and not item.acl.is_set()
    )
    cache_key = parent_id
    if use_cache and cache_key in visibility_cache:
        # print('using cache', item)
        return visibility_cache[cache_key]

    # update access map if needed
    access_map = ctx_access_map.get()
    if item.parent_id is None:
        # root container
        access_map[item.id] = item.access_record
    elif parent_id not in access_map:
        # print('fetching', parent_id)
        container_id = item.id if item.is_collection else parent_id
        results = await connector.fetch_access_map(container_id)
        access_map.update({
            row['id']: AccessRecord(row['parent_id'],
                                    row['acl'] and
                                    orjson.loads(row['acl']),
                                    row['is_deleted'],
                                    row['expires_at'])
            for row in results
            if row['id'] not in access_map
        })

    is_visible = True

    if not item.is_system:
        # check recycled / expired
        deleted = _is_deleted(item)
        if deleted:
            is_visible = False
        elif not connector.supports_ttl:
            expired = _is_expired(item)
            if expired:
                is_visible = False

    # can_read = can_read and await item.can_read(user)

    if use_cache:
        visibility_cache[cache_key] = is_visible

    return is_visible


class Roles:
    # 0 - no access
    # 1 - read
    # 2 - update, delete if owner
    # 4 - update, delete anyway
    # 8 - full control
    NO_ACCESS = 0
    READER = 1
    AUTHOR = 2
    CONTENT_CO = 4
    COORDINATOR = 8

    @staticmethod
    async def resolve(item, membership):
        acl = item.effective_acl
        member_of = set()
        if membership is not None:
            if await membership.is_admin():
                return Roles.COORDINATOR
            if membership.id in acl:
                return acl[membership.id]

            # get membership
            member_of.update({
                group_id
                for group_id in await membership.member_of.ids()
            })

            if member_of:
                # resolve nested groups membership
                member_of.update(
                    await Roles._resolve_membership(frozenset(member_of))
                )

            if hasattr(membership, 'authenticate'):
                member_of.add('authusers')

        # last add everyone
        member_of.add('everyone')

        perms = [acl.get(group_id, Roles.NO_ACCESS) for group_id in member_of]
        if not perms:
            return Roles.NO_ACCESS
        return max(perms)

    @staticmethod
    @context_cacheable(1024)
    async def _resolve_membership(group_ids: frozenset) -> set:
        extended_membership = set()
        # groups = [r async for r in db_connector().get_multi(group_ids)]
        async for group in db_connector().get_multi(group_ids):
            extended_membership.update({
                group_id for group_id in await group.member_of.ids()
            })
        if extended_membership:
            extended_membership.update(
                await Roles._resolve_membership(frozenset(extended_membership))
            )
        return extended_membership
