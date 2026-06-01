import time
import json
import asyncio
import sqlite3
from datetime import datetime
from logger import setup_logger


class DBWriter:
    def __init__(self, conn: sqlite3.Connection, queue: asyncio.Queue):
        self.conn = conn
        self.queue = queue
        self.cursor = conn.cursor()

        self.logger = setup_logger("DBWriter")

        # -------------------------
        # BATCH BUFFERS
        # -------------------------
        self.trades_buffer = []
        self.ob_buffer = []
        self.news_buffer = []

        # -------------------------
        # NEWS FLUSH CONTROL
        # -------------------------
        self.last_news_time = 0
        self.news_flush_task = None

    # =========================================================
    # MAIN LOOP
    # =========================================================
    async def run(self):
        while True:
            try:
                item = await self.queue.get()
                table = item["table"]
                data = item["data"]

                if table == "trades":
                    self._handle_trades(data)

                elif table == "orderbooks":
                    self._handle_orderbooks(data)

                elif table == "news":
                    await self._handle_news(data)

                elif table == "open_interest":
                    self._insert_open_interest(data)

                elif table == "ohlcv":
                    self._insert_ohlcv(data)

            except Exception as e:
                self.logger.error(f"DB writer error: {e}")

    # =========================================================
    # TRADES (batch 200)
    # =========================================================
    def _handle_trades(self, data):
        self.trades_buffer.append(data)

        if len(self.trades_buffer) >= 200:
            self._flush_trades()

    def _flush_trades(self):
        rows = [
            (
                d["session_pair_id"],
                datetime.utcfromtimestamp(d["T"] / 1000).isoformat(),
                float(d["p"]),
                float(d["q"]),
                1 if d.get("m") else 0,
                json.dumps(d)
            )
            for d in self.trades_buffer
        ]

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
        """, rows)

        self.conn.commit()
        self.trades_buffer.clear()

    # =========================================================
    # ORDERBOOKS (batch 200)
    # =========================================================
    def _handle_orderbooks(self, data):
        self.ob_buffer.append(data)

        if len(self.ob_buffer) >= 100:
            self._flush_orderbooks()

    def _flush_orderbooks(self):
        rows = [
            (
                d["session_pair_id"],
                datetime.utcfromtimestamp(d["E"] / 1000).isoformat(),
                json.dumps(d["bids"]),
                json.dumps(d["asks"]),
                json.dumps(d)
            )
            for d in self.ob_buffer
        ]

        self.cursor.executemany("""
            INSERT INTO orderbooks (
                session_pair_id,
                timestamp,
                bids,
                asks,
                raw
            )
            VALUES (?, ?, ?, ?, ?)
        """, rows)

        self.conn.commit()
        self.ob_buffer.clear()

    # =========================================================
    # NEWS (burst batching + time flush)
    # =========================================================
    async def _handle_news(self, data):
        self.news_buffer.append(data)
        self.last_news_time = time.time()

        # restart flush timer
        if self.news_flush_task:
            self.news_flush_task.cancel()

        self.news_flush_task = asyncio.create_task(self._news_flush_timer())

    async def _news_flush_timer(self):
        try:
            await asyncio.sleep(2)

            # ha azóta nem jött új news → flush
            if time.time() - self.last_news_time >= 2:
                if self.news_buffer:
                    self._flush_news()

        except asyncio.CancelledError:
            pass

    def _flush_news(self):
        rows = [
            (
                d["session_pair_id"],
                d["id"],
                d.get("category"),
                datetime.utcfromtimestamp(d["time"] / 1000).isoformat(),
                d.get("headline"),
                d.get("summary")
            )
            for d in self.news_buffer
        ]

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
        """, rows)

        self.conn.commit()
        self.news_buffer.clear()

    # =========================================================
    # OPEN INTEREST (immediate)
    # =========================================================
    def _insert_open_interest(self, data):
        self.cursor.execute("""
            INSERT INTO open_interest (
                session_pair_id,
                timestamp,
                open_interest,
                raw
            )
            VALUES (?, ?, ?, ?)
        """, (
            data["session_pair_id"],
            datetime.utcfromtimestamp(data["time"] / 1000).isoformat(),
            float(data["openInterest"]),
            json.dumps(data)
        ))

        self.conn.commit()

    # =========================================================
    # OHLCV (immediate)
    # =========================================================
    def _insert_ohlcv(self, data):
        for c in data["candles"]:
            self.cursor.execute("""
                INSERT INTO ohlcv (
                    session_pair_id,
                    interval,
                    period,
                    open_time,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    raw
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data["session_pair_id"],
                data["interval"],
                data.get("period"),
                datetime.utcfromtimestamp(c["open_time"] / 1000).isoformat(),
                float(c["open"]),
                float(c["high"]),
                float(c["low"]),
                float(c["close"]),
                float(c["volume"]),
                json.dumps(c)
            ))

        self.conn.commit()