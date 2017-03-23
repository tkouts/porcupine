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


# from porcupine.utils import system


# class BlobValue:
#
#     def __init__(self, descriptor, instance):
#         self._descriptor = descriptor
#         self._instance = instance
#
#     async def get(self):
#         storage = getattr(self._instance, self._descriptor.storage)
#         name = self._descriptor.name
#         if name not in storage:
#             value = await db.connector.get_external('{0}_{1}'.format(
#                 self._instance.id, name))
#             storage[name] = value
#         return storage[name]
#
#     async def set(self, value):
#         # should_snapshot = False
#         name = self._descriptor.name
#         if name not in self._instance.__snapshot__:
#             # do not keep previous value, just trigger on_change
#             self._instance.__snapshot__[name] = None
#         DataType.__set__(self._descriptor, self._instance, value)


class Blob(DataType):
    """
    Base class for binary large objects.
    """
    safe_type = bytes
    allow_none = True
    storage = '__externals__'

    def __init__(self, default=None, **kwargs):
        super().__init__(default, **kwargs)

    async def fetch(self, instance, set_storage=True):
        name = self.name
        value = await db.connector.get_external('{0}_{1}'.format(
            instance.id, name))
        if set_storage:
            storage = getattr(instance, self.storage)
            storage[name] = value
        return value

    def __get__(self, instance, owner):
        if instance is None:
            return self
        storage = getattr(instance, self.storage)
        if self.name in storage:
            future = asyncio.Future()
            future.set_result(storage[self.name])
            return future
        return self.fetch(instance)

    def snapshot(self, instance, value):
        if self.name not in instance.__snapshot__:
            instance.__snapshot__[self.name] = None

    def clone(self, instance, memo):
        pass

    def on_change(self, instance, value, old_value):
        if value is not None:
            context.txn.put_external(
                '{0}_{1}'.format(instance.id, self.name), value)

    def on_delete(self, instance, value, is_permanent):
        if is_permanent:
            context.txn.delete_external(
                '{0}_{1}'.format(instance.id, self.name))


class Text(Blob):
    """Data type to use for large text streams"""
    safe_type = str


# class FileValue(ExternalAttributeValue):
#
#     @property
#     def filename(self):
#         return self._datum['filename']
#
#     @filename.setter
#     def filename(self, filename):
#         self._datum['filename'] = filename
#
#     def get_file(self):
#         return io.StringIO(self.read())
#
#     def load_from_file(self, filename):
#         """
#         This method sets the value property of this data type instance
#         to a stream read from a file that resides on the file system.
#
#         @param filename: A valid filename
#         @type filename: str
#
#         @return: None
#         """
#         with open(filename, 'rb') as f:
#             self.write(f.read())


class File(Blob):
    """Data type to use for file objects"""

    # def __get__(self, instance, owner):
    #     if instance is None:
    #         return self
    #     name = self.name
    #
    #     value = None
    #     if name not in instance.__storage__:
    #         instance.__storage__[name] = {
    #             'id': system.generate_oid(),
    #             'size': len(self.default),
    #             'filename': ''
    #         }
    #         value = self.default
    #     return FileValue(self, instance.__storage__[name], value)


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
