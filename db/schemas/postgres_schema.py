from db.schemas.base_schema import BaseSchema


class PostgresSchema(BaseSchema):

    def _create_sessions(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL
        )
        """)

    def _create_logs(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,

            session_id INTEGER NOT NULL,

            filename TEXT NOT NULL,

            created_at TIMESTAMP NOT NULL,

            FOREIGN KEY(session_id)
                REFERENCES sessions(id)
                ON DELETE CASCADE
        )
        """)

    def _create_session_pairs(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS session_pairs (
            id SERIAL PRIMARY KEY,
            session_id INTEGER NOT NULL,
            pair TEXT NOT NULL,
            UNIQUE(session_id, pair),
            FOREIGN KEY(session_id) REFERENCES sessions(id)
        )
        """)

    def _create_trades(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            session_pair_id INTEGER NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            price DOUBLE PRECISION,
            quantity DOUBLE PRECISION,
            is_buyer_maker BOOLEAN,
            raw JSONB,
            FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
        )
        """)

        # performance index (IMPORTANT)
        self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_trades_pair_time
        ON trades(session_pair_id, timestamp)
        """)

    def _create_orderbooks(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS orderbooks (
            id SERIAL PRIMARY KEY,
            session_pair_id INTEGER NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            bids JSONB NOT NULL,
            asks JSONB NOT NULL,
            raw JSONB,
            FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
        )
        """)

        self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_orderbooks_pair_time
        ON orderbooks(session_pair_id, timestamp)
        """)

    def _create_news(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            session_pair_id INTEGER NOT NULL,
            external_id BIGINT,
            category TEXT,
            timestamp TIMESTAMP NOT NULL,
            headline TEXT,
            summary TEXT,
            FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
        )
        """)

        self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_news_pair_time
        ON news(session_pair_id, timestamp)
        """)

    def _create_open_interest(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS open_interest (
            id SERIAL PRIMARY KEY,
            session_pair_id INTEGER NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open_interest DOUBLE PRECISION NOT NULL,
            raw JSONB,
            FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
        )
        """)

    def _create_ohlcv(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            id SERIAL PRIMARY KEY,
            session_pair_id INTEGER NOT NULL,
            interval TEXT NOT NULL,
            period TEXT,
            open_time TIMESTAMP NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            raw JSONB,
            FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
        )
        """)

        self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlcv_pair_time
        ON ohlcv(session_pair_id, open_time)
        """)