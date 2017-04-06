import abc


class AbstractIndex(object, metaclass=abc.ABCMeta):
    def __init__(self, connector, data_type):
        self.connector = connector
        self.data_type = data_type

    @property
    def name(self):
        return self.data_type.name

    @property
    def key(self):
        return self.data_type.storage_key

    @abc.abstractmethod
    def exists(self, container_id, value):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
