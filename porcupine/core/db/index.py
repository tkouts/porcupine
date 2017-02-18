import abc


class AbstractIndex(object, metaclass=abc.ABCMeta):
    def __init__(self, connector, name, unique):
        self.connector = connector
        self.name = name
        self.unique = unique

    @abc.abstractmethod
    def exists(self, container_id, value):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
