from porcupine.core.services import db_connector


class SchemaMaintenanceTask:
    """
    Base schema maintenance task
    """
    __slots__ = ('key', 'connector')

    def __init__(self, key):
        self.key = key
        self.connector = db_connector()

    async def execute(self):
        ...
