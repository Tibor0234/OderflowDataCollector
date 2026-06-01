import sqlite3


def init_db(conn: sqlite3.Connection):
    cursor = conn.cursor()

    # sessions
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL
    )
    """)

    # session_pairs
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS session_pairs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER NOT NULL,
        pair TEXT NOT NULL,
        FOREIGN KEY(session_id) REFERENCES sessions(id),
        UNIQUE(session_id, pair)
    )
    """)

    # news
    cursor.execute("""
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

    # trades
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        session_pair_id INTEGER NOT NULL,

        timestamp TEXT NOT NULL,

        price REAL NOT NULL,
        quantity REAL NOT NULL,

        is_buyer_maker INTEGER,

        raw TEXT,

        FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
    )
    """)

    # open interest
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS open_interest (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        session_pair_id INTEGER NOT NULL,

        timestamp TEXT NOT NULL,

        open_interest REAL NOT NULL,

        raw TEXT,

        FOREIGN KEY(session_pair_id) REFERENCES session_pairs(id)
    )
    """)

    # orderbook
    cursor.execute("""
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

    # ohlcv
    cursor.execute("""
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

    conn.commit()