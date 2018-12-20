from porcupine.core.services import get_service


class SchemaMaintenanceTask:
    """
    Base schema maintenance task
    """
    __slots__ = ('key', 'connector')

    def __init__(self, key):
        self.key = key
        self.connector = get_service('db').connector

    async def execute(self):
        ...
