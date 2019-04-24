from porcupine.core.services import db_connector


class SchemaMaintenanceTask:
    """
    Base schema maintenance task
    """
    __slots__ = 'key'

    def __init__(self, key):
        self.key = key

    @property
    def connector(self):
        return db_connector()

    async def execute(self):
        ...
