import abc


class AbstractTransaction(object, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, connector):
        self.connector = connector
        self.connector.active_txns += 1

    @abc.abstractmethod
    def _retry(self):
        self.connector.active_txns += 1

    @abc.abstractmethod
    def commit(self):
        """
        Commits the transaction.

        @return: None
        """
        self.connector.active_txns -= 1

    @abc.abstractmethod
    def abort(self):
        """
        Aborts the transaction.

        @return: None
        """
        self.connector.active_txns -= 1
