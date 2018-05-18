"""
Helper module for resolving object permissions
"""
from porcupine import db
from porcupine.core.context import context_cacheable

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


async def resolve(item, user) -> int:
    acl = await item.effective_acl
    return await resolve_acl(acl, user)


async def resolve_acl(acl, user_or_group) -> int:
    # print(acl, user_or_group)
    member_of = set()
    if user_or_group is not None:
        if await user_or_group.is_admin():
            return COORDINATOR
        if user_or_group.id in acl:
            return acl[user_or_group.id]

        # get membership
        member_of.update({group_id
                          async for group_id in user_or_group.member_of})

        if member_of:
            # resolve nested groups membership
            member_of.update(await resolve_membership(frozenset(member_of)))

        if hasattr(user_or_group, 'authenticate'):
            member_of.add('authusers')

    # add everyone
    member_of.add('everyone')

    perms = [acl.get(group_id, NO_ACCESS) for group_id in member_of]
    if not perms:
        return NO_ACCESS
    return max(perms)


@context_cacheable(1000)
async def resolve_membership(group_ids: frozenset) -> set:
    extended_membership = set()
    groups = [g async for g in db.connector.get_multi(group_ids) if g]
    for group in groups:
        extended_membership.update({
            group_id async for group_id in group.member_of})
    if extended_membership:
        extended_membership.update(
            await resolve_membership(frozenset(extended_membership)))
    return extended_membership
