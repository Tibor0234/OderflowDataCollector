import sqlite3
from db.writers.base_writer import BaseDBWriter


class SQLiteDBWriter(BaseDBWriter):
    def __init__(self, conn: sqlite3.Connection, queue):
        super().__init__(queue)
        self.conn = conn
        self.cursor = conn.cursor()

    def _execute_trades(self, data):
        self.cursor.executemany("""
            INSERT INTO trades (
                session_pair_id,
                timestamp,
                price,
                quantity,
                is_buyer_maker,
                raw
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, data)

        self.conn.commit()

    def _execute_orderbooks(self, data):
        self.cursor.executemany("""
            INSERT INTO orderbooks (
                session_pair_id,
                timestamp,
                bids,
                asks,
                raw
            )
            VALUES (?, ?, ?, ?, ?)
        """, data)

        self.conn.commit()

    def _execute_news(self, data):
        self.cursor.executemany("""
            INSERT INTO news (
                session_pair_id,
                external_id,
                category,
                timestamp,
                headline,
                summary
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, data)

        self.conn.commit()

    def _execute_open_interest(self, data):
        self.cursor.execute("""
            INSERT INTO open_interest (
                session_pair_id,
                timestamp,
                open_interest,
                raw
            )
            VALUES (?, ?, ?, ?)
        """, data)

        self.conn.commit()

    def _execute_ohlcv(
        self,
        session_pair_id,
        interval,
        period,
        timestamp,
        data
    ):
        self.cursor.execute("""
            INSERT INTO ohlcv_fetches (
                session_pair_id, interval, period, timestamp
            )
            VALUES (?, ?, ?, ?)
        """, (session_pair_id, interval, period, timestamp))

        fetch_id = self.cursor.lastrowid

        self.cursor.executemany("""
            INSERT INTO ohlcv (
                fetch_id,
                open_time,
                open,
                high,
                low,
                close,
                volume,
                raw
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (fetch_id, *row)
            for row in data
        ])

        self.conn.commit()