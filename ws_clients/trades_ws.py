import asyncio
import time
import websockets
import json
import random
from decimal import Decimal
from db.writer_queue import WriterQueue
from logger import LoggerManager


class TradesWS:
    def __init__(self, pair, writer: WriterQueue, session_pair_id: int):
        self.pair = pair
        self.writer = writer
        self.session_pair_id = session_pair_id

        self.logger = LoggerManager().get_logger(f"{pair.upper()}_Trades-WS")

        self.ws_url = f"wss://fstream.binance.com/ws/{pair.lower()}@trade"

        self.buffer = []

        self.logger.info(f"Initialized WS: {self.ws_url}")

        self.ws_alive = False
        self.msg_count = 0
        self.last_msg_time = 0

    async def run(self):
        backoff = 1

        heartbeat_task = asyncio.create_task(self._heartbeat())

        try:
            while True:
                try:
                    self.logger.info("Connecting WebSocket...")
                    self.ws_alive = False

                    async with websockets.connect(self.ws_url) as ws:
                        self.logger.info("Connected to WebSocket")

                        self.ws_alive = True
                        backoff = 1

                        while True:
                            raw = await ws.recv()
                            data = json.loads(raw)

                            self.msg_count += 1
                            self.last_msg_time = time.time()

                            price = Decimal(data["p"])
                            qty = Decimal(data["q"])

                            if price == 0 or qty == 0:
                                continue

                            self.buffer.append(data)

                            if len(self.buffer) >= 200:
                                await self.writer.push(
                                    "trades",
                                    self.buffer,
                                    self.session_pair_id
                                )
                                self.buffer.clear()

                except Exception as e:
                    self.ws_alive = False
                    self.logger.warning(f"WS error: {e}")

                    if self.buffer:
                        await self.writer.push(
                            "trades",
                            self.buffer,
                            self.session_pair_id
                        )
                        self.buffer.clear()

                    sleep_time = min(backoff, 60)
                    await asyncio.sleep(sleep_time + random.random())

                    backoff *= 2

        finally:
            heartbeat_task.cancel()

    
    async def _heartbeat(self, rate=60):
        self.msg_count = 0
        last = 0

        while True:
            await asyncio.sleep(rate)

            if not self.ws_alive:
                self.logger.warning("Stream DOWN")
                continue

            delta = self.msg_count - last
            last = self.msg_count

            lag = time.time() - self.last_msg_time

            self.logger.info(
                f"Stream alive | msgs={self.msg_count} | rate={delta}/{rate}s | lag={lag:.1f}s | buffer={len(self.buffer)}"
            )