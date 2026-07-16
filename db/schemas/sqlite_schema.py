from db.schemas.base_schema import BaseSchema

class SQLiteSchema(BaseSchema):

    def _create_sessions(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL
        )
        """)

    def _create_logs(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            session_id INTEGER NOT NULL,

            filename TEXT NOT NULL,

            created_at TEXT NOT NULL,

            FOREIGN KEY(session_id)
                REFERENCES sessions(id)
                ON DELETE CASCADE
        )
        """)

    def _create_session_pairs(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS session_pairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            pair TEXT NOT NULL,
            UNIQUE(session_id, pair)
        )
        """)

    def _create_trades(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_pair_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            price REAL,
            quantity REAL,
            is_buyer_maker INTEGER,
            raw TEXT
        )
        """)

    def _create_orderbooks(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS orderbooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            session_pair_id INTEGER NOT NULL,

            timestamp TEXT NOT NULL,

            bids TEXT NOT NULL,
            asks TEXT NOT NULL,

            raw TEXT,

            FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
        )
        """)

    def _create_news(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            session_pair_id INTEGER NOT NULL,

            external_id INTEGER,
            category TEXT,

            timestamp TEXT NOT NULL,

            headline TEXT,
            summary TEXT,

            FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
        )
        """)

    def _create_open_interest(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS open_interest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            session_pair_id INTEGER NOT NULL,

            timestamp TEXT NOT NULL,

            open_interest REAL NOT NULL,

            raw TEXT,

            FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
        )
        """)

    def _create_ohlcv(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            session_pair_id INTEGER NOT NULL,

            interval TEXT NOT NULL,
            period TEXT,

            open_time TEXT NOT NULL,

            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,

            raw TEXT,

            FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
        )
        """)