"""
Helper module for resolving object permissions
"""

# from porcupine.core.cache import context_cacheable

# 1 - read
# 2 - update, delete if owner
# 4 - update, delete anyway
# 8 - full control
NO_ACCESS = 0
READER = 1
AUTHOR = 2
CONTENT_CO = 4
COORDINATOR = 8


def resolve(item, user):
    return get_role(item.security, user)


def get_role(security_descriptor, user_or_group):
    if user_or_group.is_admin():
        return COORDINATOR
    if user_or_group.id in security_descriptor:
        return security_descriptor[user_or_group.id]
    member_of = ['everyone']
    member_of.extend(user_or_group.memberof)
    if hasattr(user_or_group, 'authenticate'):
        member_of.extend(['authusers'])
    # resolve nested groups membership
    member_of.extend(resolve_membership(tuple(member_of)))
    perms = [security_descriptor.get(group_id, NO_ACCESS)
             for group_id in member_of] or [NO_ACCESS]
    return max(perms)


# @context_cacheable
def resolve_membership(group_ids):
    from porcupine import db
    extended_membership = []
    groups = db._db.get_multi(group_ids, get_lock=False)
    for group in groups:
        extended_membership += [group_id for group_id in group.memberof
                                if group_id not in group_ids]
    if extended_membership:
        extended_membership += resolve_membership(
            tuple(set(extended_membership)))
    return extended_membership
