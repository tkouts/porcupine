from .cursor import AbstractCursor


class Join(AbstractCursor):
    """
    Helper cursor for performing joins on indexed attributes
    """

    def __init__(self, connector, cursor_list):
        super().__init__(connector, None)
        self._cursor_list = cursor_list

    def set_scope(self, scope):
        [c.set_scope(scope) for c in self._cursor_list]
        self._scope = scope

    # def duplicate(self):
    #     clone = copy.copy(self)
    #     clone._cur_list = [cur.duplicate() for cur in self._cur_list]
    #     return clone

    @property
    def size(self):
        return 0

    def reverse(self):
        self._reversed = not self._reversed
        [c.reverse() for c in self._cursor_list]

    def _optimize(self):
        sizes = [c.size for c in self._cursor_list]
        if not all(sizes):
            return None, None
        cursors = list(zip(sizes, self._cursor_list))
        cursors.sort()
        base_cursor = cursors[0][1]
        rte_cursors = [c[1] for c in cursors[1:]]
        return base_cursor, rte_cursors

    def __iter__(self):
        cursor, rte_cursors = self._optimize()
        if cursor:
            cursor.enforce_permissions = self.enforce_permissions
            cursor.fetch_mode = 1
            for item in cursor:
                is_valid = all([c.eval(item) for c in rte_cursors])
                if is_valid:
                    if self.fetch_mode == 0:
                        yield item.id
                    elif self.fetch_mode == 1:
                        yield item

    def close(self):
        for cursor in self._cursor_list:
            cursor.close()
