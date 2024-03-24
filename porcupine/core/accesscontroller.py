import orjson
import time
from collections import namedtuple, ChainMap
# from porcupine.core.services import db_connector
# from porcupine.core.schema.partial import PartialItem
# from porcupine import db
from porcupine.core.context import (
    ctx_access_map,
    # ctx_user,
    ctx_db,
    ctx_sys,
    # ctx_visibility_cache,
    ctx_membership_cache,
    # context_cacheable,
    # context_user
)

AccessRecord = namedtuple(
    'AccessRecord',
    ['parent_id', 'acl']
)


def _iter_access_item(parent_id, access_item):
    access_map = ctx_access_map.get()
    while parent_id is not None:
        access_record = access_map[parent_id]
        yield getattr(access_record, access_item)
        parent_id = access_record.parent_id


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


def is_contained_in(item, container):
    if item.parent_id is not None:
        parent_ids = _iter_access_item(item.parent_id, 'parent_id')
        for parent_id in parent_ids:
            if parent_id == container.id:
                return True
    return False


def get_ancestor_id(item, n_levels: int):
    if n_levels == 1:
        return item.parent_id
    if item.parent_id is not None:
        n_levels -= 2
        parent_ids = _iter_access_item(item.parent_id, 'parent_id')
        for i, parent_id in enumerate(parent_ids):
            if i == n_levels:
                return parent_id
    return None


def resolve_visibility(item) -> bool:
    if item.__is_new__ or ctx_sys.get():
        return True

    # check recycled / expired
    if item.is_deleted:
        return False
    elif not item.is_collection and item.expires_at is not None:
        return time.time() < item.expires_at

    return True


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
        # fetch access map if needed
        access_map = ctx_access_map.get()
        parent_id = item.parent_id
        if parent_id is not None and parent_id not in access_map:
            db = ctx_db.get()
            container_id = item.id if item.is_collection else parent_id
            results = await db.fetch_access_map(container_id)
            access_map.update({
                row['id']: AccessRecord(
                    row['parent_id'],
                    row['acl'] and orjson.loads(row['acl'])
                )
                for row in results
                if row['id'] not in access_map
            })
        if item.is_collection:
            # update access map
            access_map[item.id] = item.access_record

        acl = item.effective_acl
        member_of = set()
        if membership is not None:
            if await membership.is_admin():
                return Roles.COORDINATOR
            if membership.id in acl:
                return acl[membership.id]

            should_resolve_membership = [
                k for k in acl if k not in ('everyone', 'authusers')
            ]
            if should_resolve_membership:
                membership_cache = ctx_membership_cache.get()
                if membership.id in membership_cache:
                    member_of.update(membership_cache[membership.id])
                else:
                    # resolve membership
                    resolved_membership = set()
                    resolved_membership.update({
                        group_id
                        for group_id in await membership.member_of.ids()
                    })

                    if resolved_membership:
                        # resolve nested groups membership
                        resolved_membership.update(
                            await Roles._resolve_membership(frozenset(member_of))
                        )

                    membership_cache[membership.id] = resolved_membership
                    member_of.update(resolved_membership)

        if membership is not None and hasattr(membership, 'authenticate'):
            member_of.add('authusers')

        # last add everyone
        member_of.add('everyone')

        perms = [acl.get(group_id, Roles.NO_ACCESS) for group_id in member_of]
        if not perms:
            return Roles.NO_ACCESS
        return max(perms)

    @staticmethod
    async def _resolve_membership(group_ids: frozenset) -> set:
        db = ctx_db.get()
        extended_membership = set()
        async for group in db.get_multi(group_ids):
            extended_membership.update({
                group_id for group_id in await group.member_of.ids()
            })
        if extended_membership:
            extended_membership.update(
                await Roles._resolve_membership(frozenset(extended_membership))
            )
        return extended_membership
