import os
import sqlite3

from db.retention.base_retention import BaseRetention


class SQLiteRetention(BaseRetention):
    def __init__(self, conn: sqlite3.Connection, max_sessions: int):
        super().__init__(conn, max_sessions)

        self.cursor = conn.cursor()

    def get_session_count(self):
        self.cursor.execute("""
            SELECT COUNT(*)
            FROM sessions
        """)
        
        return self.cursor.fetchone()[0]

    def get_oldest_session(self):
        self.cursor.execute("""
            SELECT id
            FROM sessions
            ORDER BY created_at ASC
            LIMIT 1
        """)

        row = self.cursor.fetchone()

        return row[0] if row else None

    def delete_session(self, session_id):
        # session_pair id-k
        self.cursor.execute("""
            SELECT id
            FROM session_pairs
            WHERE session_id = ?
        """, (session_id,))

        pair_ids = [r[0] for r in self.cursor.fetchall()]

        for pair_id in pair_ids:
            self.cursor.execute(
                "DELETE FROM trades WHERE session_pair_id = ?",
                (pair_id,)
            )

            self.cursor.execute(
                "DELETE FROM orderbooks WHERE session_pair_id = ?",
                (pair_id,)
            )

            self.cursor.execute(
                "DELETE FROM open_interest WHERE session_pair_id = ?",
                (pair_id,)
            )

            self.cursor.execute(
                "DELETE FROM news WHERE session_pair_id = ?",
                (pair_id,)
            )

            self.cursor.execute(
                "DELETE FROM ohlcv WHERE session_pair_id = ?",
                (pair_id,)
            )

        # sessionhez tartozó log fájlok lekérése
        self.cursor.execute("""
            SELECT filename
            FROM logs
            WHERE session_id = ?
        """, (session_id,))

        log_paths = [r[0] for r in self.cursor.fetchall()]

        # log rekordok törlése
        self.cursor.execute("""
            DELETE FROM logs
            WHERE session_id = ?
        """, (session_id,))

        # log fájlok törlése
        for path in log_paths:
            if os.path.exists(path):
                os.remove(path)

        self.cursor.execute("""
            DELETE FROM session_pairs
            WHERE session_id = ?
        """, (session_id,))

        self.cursor.execute("""
            DELETE FROM sessions
            WHERE id = ?
        """, (session_id,))

        self.conn.commit()

        # visszaadja a felszabadított helyet az OS-nek
        self.cursor.execute("VACUUM")