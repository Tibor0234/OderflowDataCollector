import psycopg
from db.writers.base_writer import BaseDBWriter


class PostgresDBWriter(BaseDBWriter):
    def __init__(self, conn: psycopg.Connection, queue):
        super().__init__(queue)
        self.conn = conn
        self.cursor = conn.cursor()

    # =========================================================
    # TRADES
    # =========================================================
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
            VALUES (%s, %s, %s, %s, %s, %s)
        """, data)

        self.conn.commit()

    # =========================================================
    # ORDERBOOKS
    # =========================================================
    def _execute_orderbooks(self, data):
        self.cursor.executemany("""
            INSERT INTO orderbooks (
                session_pair_id,
                timestamp,
                bids,
                asks,
                raw
            )
            VALUES (%s, %s, %s, %s, %s)
        """, data)

        self.conn.commit()

    # =========================================================
    # NEWS
    # =========================================================
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
            VALUES (%s, %s, %s, %s, %s, %s)
        """, data)

        self.conn.commit()

    # =========================================================
    # OPEN INTEREST
    # =========================================================
    def _execute_open_interest(self, data):
        self.cursor.execute("""
            INSERT INTO open_interest (
                session_pair_id,
                timestamp,
                open_interest,
                raw
            )
            VALUES (%s, %s, %s, %s)
        """, data)

        self.conn.commit()

    # =========================================================
    # OHLCV
    # =========================================================
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
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (session_pair_id, interval, period, timestamp))

        fetch_id = self.cursor.fetchone()[0]

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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, [
            (fetch_id, *row)
            for row in data
        ])

        self.conn.commit()