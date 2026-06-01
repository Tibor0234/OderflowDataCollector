import sqlite3
from datetime import datetime


def create_session(conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO sessions (created_at)
        VALUES (?)
        """,
        (datetime.utcnow().isoformat(),)
    )

    conn.commit()
    return cursor.lastrowid


def create_session_pair(conn: sqlite3.Connection, session_id: int, pair: str) -> int:
    pair = pair.lower()

    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO session_pairs (session_id, pair)
        VALUES (?, ?)
        """,
        (session_id, pair)
    )

    conn.commit()

    return cursor.lastrowid