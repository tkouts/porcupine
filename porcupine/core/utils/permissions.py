"""
Helper module for resolving object permissions
"""
from porcupine.core.services import db_connector
from porcupine.core.context import context_cacheable
from porcupine.core.accesscontroller import resolve_acl

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


async def resolve(item, membership) -> int:
    acl = item.effective_acl
    member_of = set()
    if membership is not None:
        if await membership.is_admin():
            return COORDINATOR
        if membership.id in acl:
            return acl[membership.id]

        # get membership
        member_of.update({group_id
                          async for group_id in membership.member_of})

        if member_of:
            # resolve nested groups membership
            member_of.update(await resolve_membership(frozenset(member_of)))

        if hasattr(membership, 'authenticate'):
            member_of.add('authusers')

    # last add everyone
    member_of.add('everyone')

    perms = [acl.get(group_id, NO_ACCESS) for group_id in member_of]
    if not perms:
        return NO_ACCESS
    return max(perms)


@context_cacheable(1024)
async def resolve_membership(group_ids: frozenset) -> set:
    extended_membership = set()
    groups = [r async for r in db_connector().get_multi(group_ids)
              if r is not None]
    for group in groups:
        extended_membership.update({
            group_id async for group_id in group.member_of
        })
    if extended_membership:
        extended_membership.update(
            await resolve_membership(frozenset(extended_membership))
        )
    return extended_membership
