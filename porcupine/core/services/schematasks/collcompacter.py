from collections import OrderedDict

from porcupine import exceptions, log
from porcupine.core.services.schematasks.task import SchemaMaintenanceTask


class CollectionCompacter(SchemaMaintenanceTask):
    @staticmethod
    def resolve_set(raw_string: str) -> list:
        # build set
        uniques = OrderedDict()
        for oid in raw_string.split(' '):
            if oid:
                if oid.startswith('-'):
                    key = oid[1:]
                    if key in uniques:
                        del uniques[key]
                else:
                    uniques[oid] = None
        value = list(uniques.keys())
        return value

    def compact_set(self, raw_string):
        compacted = self.resolve_set(raw_string)
        return ' '.join(compacted), True

    async def execute(self):
        if self.connector.server.debug:
            log.debug('Compacting collection {0}'.format(self.key))
        try:
            success, _ = await self.connector.swap_if_not_modified(
                self.key,
                xform=self.compact_set
            )
            if not success:
                log.info('Failed to compact collection {0}'.format(self.key))
        except exceptions.NotFound:
            # the key is removed
            pass
