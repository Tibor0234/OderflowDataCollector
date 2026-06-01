import asyncio
import httpx
import time
import random
from logger import setup_logger
from db.writer_queue import WriterQueue


class OpenInterestFetcher:
    def __init__(self, pair: str, writer: WriterQueue, session_pair_id: int):
        self.pair = pair
        self.writer = writer
        self.session_pair_id = session_pair_id

        self.logger = setup_logger(f"{pair.upper()}_OI-Fetcher")

        self.url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={pair.upper()}"

        self.logger.info(f"Initialized OI fetcher: {self.url}")

        # state
        self.fail_count = 0

    async def run(self):
        timeout = httpx.Timeout(10.0)

        async with httpx.AsyncClient(timeout=timeout) as client:
            while True:
                try:
                    self.logger.info("Fetching open interest...")

                    resp = await client.get(self.url)
                    resp.raise_for_status()

                    data = resp.json()

                    self.fail_count = 0

                    await self.writer.push(
                        "open_interest",
                        data,
                        self.session_pair_id
                    )

                    await asyncio.sleep(60)

                except Exception as e:
                    self.fail_count += 1

                    self.logger.warning(
                        f"OpenInterest error (fail={self.fail_count}): {e}"
                    )

                    # exponential backoff on repeated failures
                    sleep_time = min(60, 5 * self.fail_count)

                    await asyncio.sleep(sleep_time + random.random())