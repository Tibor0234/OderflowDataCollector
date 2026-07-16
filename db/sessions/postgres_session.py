import psycopg
from datetime import datetime
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


    def create_session_pair(self, session_id: int, pair: str) -> int:
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

        self.conn.commit()

        self.logger.info(f"Created new session pair with ID: {pair_id} and pair: {pair}")

        return pair_id