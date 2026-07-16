from abc import ABC, abstractmethod

class BaseSchema(ABC):

    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()

    def init(self):
        self._create_sessions()
        self._create_logs()
        self._create_session_pairs()
        self._create_trades()
        self._create_orderbooks()
        self._create_news()
        self._create_open_interest()
        self._create_ohlcv()

        self.conn.commit()

    @abstractmethod
    def _create_sessions(self): pass

    @abstractmethod
    def _create_logs(self): pass

    @abstractmethod
    def _create_session_pairs(self): pass

    @abstractmethod
    def _create_trades(self): pass

    @abstractmethod
    def _create_orderbooks(self): pass

    @abstractmethod
    def _create_news(self): pass

    @abstractmethod
    def _create_open_interest(self): pass

    @abstractmethod
    def _create_ohlcv(self): pass