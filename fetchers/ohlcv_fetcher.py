import asyncio
import httpx
import random
from datetime import datetime, timedelta
from db.writer_queue import WriterQueue
from logger import setup_logger


class OHLCVFetcher:
    def __init__(self, pair: str, writer: WriterQueue, session_pair_id: int):
        self.pair = pair
        self.writer = writer
        self.session_pair_id = session_pair_id

        self.logger = setup_logger(f"{pair.upper()}_OHLCV-Fetcher")

        self.url = "https://fapi.binance.com/fapi/v1/klines"

        self.logger.info("Initialized OHLCV fetcher")

        self.fail_count = 0

    async def run(self):
        timeout = httpx.Timeout(10.0)

        async with httpx.AsyncClient(timeout=timeout) as client:
            while True:
                try:
                    self.logger.info("Fetching OHLCV data...")

                    now_ts = int(datetime.utcnow().timestamp())

                    # -----------------------
                    # 4H (7 days)
                    # -----------------------
                    start_4h = int(
                        (datetime.utcnow() - timedelta(days=7)).timestamp() * 1000
                    )

                    resp_4h = await client.get(self.url, params={
                        "symbol": self.pair.upper(),
                        "interval": "4h",
                        "startTime": start_4h
                    })

                    resp_4h.raise_for_status()
                    candles_4h = resp_4h.json()

                    daily_record = {
                        "period": "last_week",
                        "symbol": self.pair,
                        "interval": "4h",
                        "time": now_ts * 1000,
                        "candles": [
                            {
                                "open_time": c[0],
                                "open": c[1],
                                "high": c[2],
                                "low": c[3],
                                "close": c[4],
                                "volume": c[5]
                            }
                            for c in candles_4h
                        ]
                    }

                    # -----------------------
                    # 30M (24h)
                    # -----------------------
                    start_30m = int(
                        (datetime.utcnow() - timedelta(hours=24)).timestamp() * 1000
                    )

                    resp_30m = await client.get(self.url, params={
                        "symbol": self.pair.upper(),
                        "interval": "30m",
                        "startTime": start_30m
                    })

                    resp_30m.raise_for_status()
                    candles_30m = resp_30m.json()

                    intraday_record = {
                        "period": "last_day",
                        "symbol": self.pair,
                        "interval": "30m",
                        "time": now_ts * 1000,
                        "candles": [
                            {
                                "open_time": c[0],
                                "open": c[1],
                                "high": c[2],
                                "low": c[3],
                                "close": c[4],
                                "volume": c[5]
                            }
                            for c in candles_30m
                        ]
                    }

                    # push batch
                    await self.writer.push(
                        "ohlcv",
                        daily_record,
                        self.session_pair_id
                    )

                    await self.writer.push(
                        "ohlcv",
                        intraday_record,
                        self.session_pair_id
                    )

                    self.fail_count = 0

                    await asyncio.sleep(86400)

                except Exception as e:
                    self.fail_count += 1

                    self.logger.warning(
                        f"OHLCV error (fail={self.fail_count}): {e}"
                    )

                    sleep_time = min(60, 5 * self.fail_count)
                    await asyncio.sleep(sleep_time + random.random())