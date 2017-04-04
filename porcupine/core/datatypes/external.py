"""
Porcupine external data types
=============================
"""
import os.path
import shutil
import asyncio

from porcupine import db, context
from .common import String
from .datatype import DataType


class Blob(DataType):
    """
    Base class for binary large objects.
    """
    safe_type = bytes
    allow_none = True
    storage_info = '_blob_'
    storage = '__externals__'

    def __init__(self, default=None, **kwargs):
        # do not allow store_as for external attributes
        kwargs.pop('store_as', None)
        super().__init__(default, **kwargs)

    async def fetch(self, instance, set_storage=True):
        name = self.name
        value = await db.connector.get_external(self.key_for(instance))
        if set_storage:
            storage = getattr(instance, self.storage)
            storage[name] = value
        return value

    def __get__(self, instance, owner):
        if instance is None:
            return self
        storage = getattr(instance, self.storage)
        if self.storage_key in storage:
            future = asyncio.Future()
            future.set_result(storage[self.storage_key])
            return future
        return self.fetch(instance)

    def set_default(self, instance, value=None):
        if value is None:
            value = self._default
        super().set_default(instance, value)
        # add external info
        setattr(instance.__storage__, self.name, self.storage_info)

    def key_for(self, instance):
        return '{0}/{1}'.format(instance.id, self.name)

    def snapshot(self, instance, value):
        if self.name not in instance.__snapshot__:
            if not instance.__is_new__ or value:
                instance.__snapshot__[self.name] = None

    def clone(self, instance, memo):
        pass

    def on_change(self, instance, value, old_value):
        if value is not None:
            context.txn.put_external(self.key_for(instance), value)

    def on_delete(self, instance, value, is_permanent):
        if is_permanent:
            context.txn.delete_external(self.key_for(instance))


class Text(Blob):
    """Data type to use for large text streams"""
    safe_type = str


class File(Blob):
    """Data type to use for file objects"""


class ExternalFileValue(str):

    def get_file(self, mode='rb'):
        return open(self, mode)


class ExternalFile(String):
    """
    Data type for linking external files. Its value
    is a string which contains the path to the file.
    """
    safe_type = str
    allow_none = True
    remove_file_on_deletion = True

    def __init__(self, default=None, **kwargs):
        super(ExternalFile, self).__init__(default, **kwargs)
        if 'remove_file_on_deletion' in kwargs:
            self.remove_file_on_deletion = kwargs['remove_file_on_deletion']

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

    def on_delete(self, instance, value, is_permanent):
        if is_permanent and self.remove_file_on_deletion:
            try:
                os.remove(value)
            except OSError:
                pass
