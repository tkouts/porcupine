from porcupine import exceptions
from porcupine.core.services import db_connector
from porcupine.core import utils


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


class CollectionMaintenanceTask(SchemaMaintenanceTask):
    __slots__ = 'ttl', 'item_id', 'chunk_no', 'collection_name'

    def __init__(self, key, ttl):
        super().__init__(key)
        self.ttl = ttl
        self.item_id, self.collection_name, chunk_no = self.key.split('/')
        self.chunk_no = int(chunk_no)

    async def bump_up_active_chunk(self):
        connector = self.connector
        counter_path = utils.get_active_chunk_key(self.collection_name)
        try:
            new_chunk_key = utils.get_collection_key(self.item_id,
                                                     self.collection_name,
                                                     self.chunk_no + 1)
            await connector.insert_multi({new_chunk_key: ''}, ttl=self.ttl)
        except exceptions.DBAlreadyExists:
            return False
        await connector.mutate_in(
            self.item_id,
            {counter_path: (connector.SUB_DOC_UPSERT_MUT, self.chunk_no + 1)}
        )
        if self.ttl:
            await connector.touch_multi({self.item_id: self.ttl})
        return True
