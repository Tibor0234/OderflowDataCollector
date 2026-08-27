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

    def get_or_create_session(self) -> int:
        session_id = self.get_today_session()

        if session_id is not None:
            self.logger.info(f"Using existing session with ID: {session_id}")
            return session_id

        return self.create_session()

    def get_today_session(self):
        today = datetime.utcnow().date().isoformat()

        self.cursor.execute(
            """
            SELECT id
            FROM sessions
            WHERE substr(created_at, 1, 10) = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (today,)
        )
        row = self.cursor.fetchone()

        return row[0] if row else None


    def create_session_pair(self, session_id: int, pair: str, metadata: dict) -> int:
        pair = pair.lower()

        self.cursor.execute(
            """
            INSERT INTO session_pairs (session_id, pair)
            VALUES (?, ?)
            """,
            (session_id, pair)
        )
        session_pair_id = self.cursor.lastrowid

        self.cursor.execute(
            """
            INSERT INTO instrument_metadata (
                session_pair_id, symbol, contract_type, status,
                base_asset, quote_asset,
                tick_size, quantity_step, price_precision,
                quantity_precision, min_quantity, min_notional, onboard_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_pair_id,
                pair,
                metadata["contract_type"],
                metadata["status"],
                metadata["base_asset"],
                metadata["quote_asset"],
                metadata["tick_size"],
                metadata.get("quantity_step"),
                metadata.get("price_precision"),
                metadata.get("quantity_precision"),
                metadata.get("min_quantity"),
                metadata.get("min_notional"),
                metadata.get("onboard_date")
            )
        )

        self.conn.commit()

        return session_pair_id

    def get_or_create_session_pair(
        self,
        session_id: int,
        pair: str,
        metadata: dict
    ) -> int:
        pair = pair.lower()

        self.cursor.execute(
            """
            SELECT id
            FROM session_pairs
            WHERE session_id = ? AND pair = ?
            """,
            (session_id, pair)
        )
        row = self.cursor.fetchone()

        if row:
            return row[0]

        return self.create_session_pair(session_id, pair, metadata)

    def get_session_pair(self, session_id: int, pair: str):
        self.cursor.execute(
            """
            SELECT id
            FROM session_pairs
            WHERE session_id = ? AND pair = ?
            """,
            (session_id, pair.lower())
        )
        row = self.cursor.fetchone()
        return row[0] if row else None