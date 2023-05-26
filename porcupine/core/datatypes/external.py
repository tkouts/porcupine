"""
Porcupine external data types
=============================
"""
import asyncio
import os.path
import shutil

import aiofiles

from porcupine.core import utils
from porcupine.core.context import context
from porcupine.core.services import db_connector
from .common import String
from .datatype import DataType


class Blob(DataType):
    """
    Base class for binary large objects.
    """
    safe_type = bytes
    storage_info = '_blob_'
    storage = '__externals__'

    def __init__(self, default=None, **kwargs):
        super().__init__(default, allow_none=True, store_as=None,
                         **kwargs)

    async def fetch(self, instance):
        connector = db_connector()
        value = await connector.get(self.key_for(instance), fmt='bytes')
        storage = getattr(instance, self.storage)
        setattr(storage, self.name, value)
        return value

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super().__get__(instance, owner)
        if value is not None:
            future = asyncio.Future()
            future.set_result(value)
            return future
        return self.fetch(instance)

    def key_for(self, instance):
        return utils.get_blob_key(instance.id, self.name)

    def snapshot(self, instance, new_value, old_value):
        # unconditional snapshot
        instance.__snapshot__[self.name] = new_value

    def clone(self, instance, memo):
        pass

    async def on_create(self, instance, value):
        await super().on_create(instance, value)
        if value is not None:
            context.txn.insert_external(self.key_for(instance), value,
                                        await instance.ttl)

    async def on_change(self, instance, value, old_value):
        await super().on_change(instance, value, old_value)
        if value is not None:
            context.txn.put_external(self.key_for(instance), value,
                                     await instance.ttl)
        else:
            await self.on_delete(instance, value)

    async def on_delete(self, instance, value):
        context.txn.delete_external(self.key_for(instance))


class Text(Blob):
    """Data type to use for large text streams"""
    safe_type = str


class File(Blob):
    """Data type to use for file objects"""


class ExternalFileValue(str):

    def get_file(self, mode='rb'):
        return aiofiles.open(self, mode)


class ExternalFile(String):
    """
    Data type for linking external files. Its value
    is a string which contains the path to the file.
    """

    def __init__(self, default=None, remove_file_on_deletion=True, **kwargs):
        super().__init__(default, allow_none=True, **kwargs)
        self.remove_file_on_deletion = remove_file_on_deletion

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = super(ExternalFile, self).__get__(instance, owner)
        if value is not None:
            return ExternalFileValue(value)

    def clone(self, instance, memo):
        duplicate_files = memo.get('_dup_ext_', False)
        if duplicate_files:
            # copy the external file
            file_counter = 1
            old_filename = new_filename = self.__get__(
                instance, instance.__class__)
            filename, extension = os.path.splitext(old_filename)
            filename = filename.split('_')[0]
            while os.path.exists(new_filename):
                new_filename = '{0}_{1}{2}'.format(
                    filename, file_counter, extension)
                file_counter += 1
            shutil.copyfile(old_filename, new_filename)
            self.__set__(instance, new_filename)

    def on_delete(self, instance, value):
        if self.remove_file_on_deletion:
            try:
                os.remove(value)
            except OSError:
                pass
