import sqlite3
from datetime import datetime
from db.sessions.base_session import BaseSession
from logger import LoggerManager


class SQLiteSession(BaseSession):

    def create_session(self) -> int:
        self.cursor.execute(
            """
            INSERT INTO sessions (created_at)
            VALUES (?)
            """,
            (datetime.utcnow().isoformat(),)
        )

        session_id = self.cursor.lastrowid

        self.cursor.execute(
            """
            INSERT INTO logs (
                session_id,
                filename,
                created_at
            )
            VALUES (?, ?, ?)
            """,
            (
                session_id,
                LoggerManager().get_log_path(),
                datetime.utcnow().isoformat()
            )
        )

        self.conn.commit()

        self.logger.info(f"Created new session with ID: {session_id}")

        return session_id


    def create_session_pair(self, session_id: int, pair: str) -> int:
        pair = pair.lower()

        self.cursor.execute(
            """
            INSERT INTO session_pairs (session_id, pair)
            VALUES (?, ?)
            """,
            (session_id, pair)
        )

        self.conn.commit()

        return self.cursor.lastrowid