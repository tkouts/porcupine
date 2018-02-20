class SchemaMaintenanceTask:
    """
    Base schema maintenance task
    """
    __slots__ = 'key'

    def __init__(self, key):
        self.key = key

    async def execute(self):
        ...
