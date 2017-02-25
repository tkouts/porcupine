"""
Porcupine external data types
=============================
"""
import os.path
import shutil
import io

from porcupine import db
from .common import DataType, String
from porcupine.utils import system


class ExternalStreamValue(object):

    def __init__(self, descriptor, datum, value=None):
        self._datum = datum
        self.__value = value
        self.__descriptor = descriptor

    def read(self):
        stream = self.__value or db._db.get_external(self._datum['id'])
        return stream

    def write(self, value):
        self.__descriptor.validate_value(value)
        db._db.put_external(self._datum['id'], value)
        self.__value = value
        self._datum['size'] = len(value)

    @property
    def id(self):
        return self._datum['id']

    @property
    def size(self):
        return self._datum['size']

    def __len__(self):
        return self.size


class ExternalAttribute(DataType):
    """
    Subclass I{ExternalAttribute} when dealing with large attribute lengths.
    These kind of attributes are not stored on the same database as
    all other object attributes.
    """
    safe_type = bytes

    def __init__(self, default='', **kwargs):
        super(ExternalAttribute, self).__init__(default, **kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        name = self.name

        value = None
        if name not in instance._dict['bag']:
            instance._dict['bag'][name] = {
                'id': system.generate_oid(),
                'size': len(self.default)
            }
            value = self.default
        return ExternalStreamValue(self, instance._dict['bag'][name], value)

    def __set__(self, instance, value):
        raise AttributeError(
            'External attributes do not support direct assignment. '
            'Use the write method instead.')

    def validate(self, instance):
        if self.required and self.__get__(instance, None).size == 0:
            raise ValueError(self.__class__.__name__, 'Attribute is mandatory')

    def clone(self, instance, memo):
        duplicate = memo.get('_dup_ext_', False)
        if duplicate:
            name = self.name
            old_id = instance._dict['bag'][name]['id']
            # generate new id
            instance._dict['bag'][name]['id'] = system.generate_oid()
            stream = self.__get__(instance, instance.__class__)
            stream.write(db._db.get_external(old_id))

    def on_delete(self, instance, value, is_permanent):
        if is_permanent:
            db._db.delete_external(value.id)


class Text(ExternalAttribute):
    """Data type to use for large text streams"""
    safe_type = str


class FileValue(ExternalStreamValue):

    @property
    def filename(self):
        return self._datum['filename']

    @filename.setter
    def filename(self, filename):
        self._datum['filename'] = filename

    def get_file(self):
        return io.StringIO(self.read())

    def load_from_file(self, filename):
        """
        This method sets the value property of this data type instance
        to a stream read from a file that resides on the file system.

        @param filename: A valid filename
        @type filename: str

        @return: None
        """
        with open(filename, 'rb') as f:
            self.write(f.read())


class File(ExternalAttribute):
    """Data type to use for file objects"""

    def __get__(self, instance, owner):
        if instance is None:
            return self
        name = self.name

        value = None
        if name not in instance._dict['bag']:
            instance._dict['bag'][name] = {
                'id': system.generate_oid(),
                'size': len(self.default),
                'filename': ''
            }
            value = self.default
        return FileValue(self, instance._dict['bag'][name], value)


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
                new_filename = '{}_{}{}'.format(
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
