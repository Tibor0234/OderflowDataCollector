import psycopg
from datetime import datetime, timedelta
from db.sessions.base_session import BaseSession
from logger import LoggerManager


class PostgresSession(BaseSession):

    def create_session(self) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO sessions (created_at)
                VALUES (%s)
                RETURNING id
                """,
                (datetime.utcnow().isoformat(),)
            )

            session_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO logs (
                    session_id,
                    filename,
                    created_at
                )
                VALUES (%s, %s, %s)
                """,
                (
                    session_id,
                    LoggerManager().get_log_path(),
                    datetime.utcnow()
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
        today = datetime.utcnow().date()
        tomorrow = today + timedelta(days=1)

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id
                FROM sessions
                WHERE created_at >= %s AND created_at < %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (today, tomorrow)
            )
            row = cursor.fetchone()

        return row[0] if row else None


    def create_session_pair(self, session_id: int, pair: str, metadata: dict) -> int:
        pair = pair.lower()

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO session_pairs (session_id, pair)
                VALUES (%s, %s)
                RETURNING id
                """,
                (session_id, pair)
            )

            pair_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO instrument_metadata (
                    session_pair_id, symbol, contract_type, status,
                    base_asset, quote_asset,
                    tick_size, quantity_step, price_precision,
                    quantity_precision, min_quantity, min_notional, onboard_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    pair_id,
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

        self.logger.info(f"Created new session pair with ID: {pair_id} and pair: {pair}")

        return pair_id

    def get_or_create_session_pair(
        self,
        session_id: int,
        pair: str,
        metadata: dict
    ) -> int:
        pair = pair.lower()

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id
                FROM session_pairs
                WHERE session_id = %s AND pair = %s
                """,
                (session_id, pair)
            )
            row = cursor.fetchone()

        if row:
            return row[0]

        return self.create_session_pair(session_id, pair, metadata)

    def get_session_pair(self, session_id: int, pair: str):
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id
                FROM session_pairs
                WHERE session_id = %s AND pair = %s
                """,
                (session_id, pair.lower())
            )
            row = cursor.fetchone()
        return row[0] if row else None