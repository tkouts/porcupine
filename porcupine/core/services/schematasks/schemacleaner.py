from porcupine import exceptions, log
from porcupine.core import utils
from porcupine.datatypes import Blob, ReferenceN, RelatorN
from porcupine.core.services.schematasks.task import SchemaMaintenanceTask


class SchemaCleaner(SchemaMaintenanceTask):
    @staticmethod
    def schema_updater(item_dict):
        from porcupine.datatypes import Blob, ReferenceN, RelatorN

        clazz = utils.get_content_class(item_dict['_cc'])
        item_schema = frozenset([key for key in item_dict.keys()
                                 if not key.startswith('_')
                                 and not key.endswith('_')])
        current_schema = frozenset([dt.storage_key
                                    for dt in clazz.__schema__.values()])
        for_removal = item_schema.difference(current_schema)
        externals = {}
        # remove old attributes
        for attr_name in for_removal:
            # detect if it is composite attribute
            attr_value = item_dict.pop(attr_name)
            # TODO: handle composites
            if isinstance(attr_value, str):
                if attr_value == Blob.storage_info:
                    externals[attr_name] = (attr_value, None)
                elif attr_value == ReferenceN.storage_info \
                        or attr_value.startswith(RelatorN.storage_info_prefix):
                    try:
                        active_chunk_key = \
                            utils.get_active_chunk_key(attr_name)
                        active_chunk = item_dict.pop(active_chunk_key)
                    except KeyError:
                        active_chunk = 0
                    externals[attr_name] = (attr_value, active_chunk)
        # update sig
        item_dict['sig'] = clazz.__sig__
        return item_dict, externals

    async def execute(self):
        try:
            success, externals = await self.connector.swap_if_not_modified(
                self.key,
                xform=self.schema_updater
            )
            if not success:
                log.info('Failed to update schema of {0}'.format(self.key))
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
                if self.connector.exists(external_key):
                    external_keys.append(external_key)
            elif ext_type == ReferenceN.storage_info \
                    or ext_type.startswith(RelatorN.storage_info_prefix):
                # item collection
                external_key = utils.get_collection_key(self.key, ext_name,
                                                        active_chunk)
                while (await self.connector.exists(external_key))[1]:
                    external_keys.append(external_key)
                    active_chunk -= 1
                    external_key = utils.get_collection_key(
                        self.key, ext_name, active_chunk)

        if external_keys:
            # TODO handle exceptions
            await self.connector.delete_multi(external_keys)
