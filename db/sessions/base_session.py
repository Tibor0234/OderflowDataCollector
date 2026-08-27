from abc import ABC, abstractmethod
from logger import LoggerManager

class BaseSession(ABC):
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()

        self.logger = LoggerManager().get_logger(self.__class__.__name__)

    @abstractmethod
    def create_session(self, conn):
        pass

    @abstractmethod
    def get_or_create_session(self):
        pass

    @abstractmethod
    def get_today_session(self):
        pass

    @abstractmethod
    def create_session_pair(self, conn, session_id, pair, metadata):
        pass

    @abstractmethod
    def get_or_create_session_pair(self, session_id, pair, metadata):
        pass

    @abstractmethod
    def get_session_pair(self, session_id, pair):
        pass
