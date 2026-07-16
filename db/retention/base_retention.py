from abc import ABC, abstractmethod

from logger import LoggerManager

class BaseRetention(ABC):
    def __init__(self, conn, max_sessions):
        self.conn = conn
        self.max_sessions = max_sessions

        self.logger = LoggerManager().get_logger(self.__class__.__name__)

    @abstractmethod
    def get_session_count(self):
        pass

    @abstractmethod
    def get_oldest_session(self):
        pass

    @abstractmethod
    def delete_session(self, session_id):
        pass

    def enforce(self):
        deleted = False

        while self.get_session_count() >= self.max_sessions:

            session_id = self.get_oldest_session()

            if session_id is None:
                break

            self.delete_session(session_id)

            deleted = True

            self.logger.info(f"Deleted session {session_id}.")

        if not deleted:
            self.logger.info("No sessions deleted.")