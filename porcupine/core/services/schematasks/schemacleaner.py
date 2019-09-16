from porcupine import exceptions, log
from porcupine.core import utils
from porcupine.datatypes import Blob, ReferenceN, RelatorN
from porcupine.core.services.schematasks.task import SchemaMaintenanceTask


class SchemaCleaner(SchemaMaintenanceTask):
    __slots__ = 'ttl'

    def __init__(self, key, ttl):
        super().__init__(key)
        self.ttl = ttl

    @staticmethod
    def schema_updater(item_dict):
        clazz = utils.get_content_class(item_dict['_cc'])
        item_schema = frozenset([key for key in item_dict.keys()
                                 if not key.startswith('_')
                                 and not key.endswith('_')])
        current_schema = frozenset([dt.storage_key
                                    for dt in clazz.__schema__.values()])
        externals = {}

        # remove old attributes
        for_removal = item_schema.difference(current_schema)
        for storage_key in for_removal:
            # detect if it is composite attribute
            attr_value = item_dict.pop(storage_key)
            # TODO: handle composites
            if isinstance(attr_value, str):
                if attr_value == Blob.storage_info:
                    externals[storage_key] = (attr_value, None)
                elif attr_value == ReferenceN.storage_info \
                        or attr_value.startswith(RelatorN.storage_info_prefix):
                    try:
                        active_chunk_key = \
                            utils.get_active_chunk_key(storage_key)
                        active_chunk = item_dict.pop(active_chunk_key)
                    except KeyError:
                        active_chunk = 0
                    externals[storage_key] = (attr_value, active_chunk)

        # add externals storage info
        for_addition = current_schema.difference(item_schema)
        for storage_key in for_addition:
            dt = utils.get_descriptor_by_storage_key(clazz, storage_key)
            if isinstance(dt, Blob):
                # print('ADDING', storage_key, dt.storage_info)
                item_dict[storage_key] = dt.storage_info

        # update sig
        item_dict['sig'] = clazz.__sig__
        return item_dict, externals

    async def execute(self):
        connector = self.connector
        if connector.server.debug:
            log.debug(f'Updating schema of {self.key}')

        try:
            success, externals = await connector.swap_if_not_modified(
                self.key,
                xform=self.schema_updater,
                ttl=self.ttl
            )
            if not success:
                log.info(f'Failed to update schema of {self.key}')
                return
        except exceptions.NotFound:
            # the key is removed
            return

        external_keys = []
        for ext_name, ext_info in externals.items():
            ext_type, active_chunk = ext_info
            if ext_type == Blob.storage_info:
                # external blob
                external_key = utils.get_blob_key(self.key, ext_name)
                _, exists = await connector.exists(external_key)
                if exists:
                    external_keys.append(external_key)
            elif ext_type == ReferenceN.storage_info \
                    or ext_type.startswith(RelatorN.storage_info_prefix):
                # item collection
                external_key = utils.get_collection_key(self.key, ext_name,
                                                        active_chunk)
                _, exists = await connector.exists(external_key)
                while exists:
                    external_keys.append(external_key)
                    active_chunk -= 1
                    external_key = utils.get_collection_key(
                        self.key, ext_name, active_chunk)
                    _, exists = await connector.exists(external_key)

        if external_keys:
            # TODO handle exceptions
            await connector.delete_multi(external_keys)
