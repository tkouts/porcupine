"""
Helper module for resolving object permissions
"""

from porcupine import db
from porcupine.core.context import context_cacheable

# 1 - read
# 2 - update, delete if owner
# 4 - update, delete anyway
# 8 - full control
NO_ACCESS = 0
READER = 1
AUTHOR = 2
CONTENT_CO = 4
COORDINATOR = 8


# def resolve(item, user):
#     return get_role(item.security, user)


async def resolve(acl, user_or_group):
    # print(security_descriptor, user_or_group)
    if await user_or_group.is_admin():
        return COORDINATOR
    if user_or_group.id in acl:
        return acl[user_or_group.id]
    member_of = ['everyone']
    member_of.extend(await user_or_group.member_of.get())
    if hasattr(user_or_group, 'authenticate'):
        member_of.append('authusers')
    # resolve nested groups membership
    member_of.extend(await resolve_membership(tuple(member_of)))
    perms = [acl.get(group_id, NO_ACCESS)
             for group_id in member_of] or [NO_ACCESS]
    return max(perms)


@context_cacheable
async def resolve_membership(group_ids):
    # from porcupine import db
    extended_membership = []
    groups = await db.connector.get_multi(group_ids)
    for group in groups:
        extended_membership += [group_id for group_id in group.member_of
                                if group_id not in group_ids]
    if extended_membership:
        extended_membership += await resolve_membership(
            tuple(set(extended_membership)))
    return extended_membership
