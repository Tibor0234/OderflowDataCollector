import asyncio
import finnhub
import random
from db.writer_queue import WriterQueue
from logger import LoggerManager


class NewsFetcher:
    def __init__(
        self,
        client: finnhub.Client,
        pair,
        writer: WriterQueue,
        session_pair_id: int,
        category="crypto"
    ):
        self.client = client
        self.pair = pair
        self.category = category

        self.writer = writer
        self.session_pair_id = session_pair_id

        self.logger = LoggerManager().get_logger(f"{pair.upper()}_News-Fetcher")

        self.last_news_id = 0

        self.logger.info(f"Initialized NewsFetcher ({category})")

        self.fail_count = 0

    async def run(self):
        first_fetch = True

        while True:
            try:
                self.logger.info("Fetching news...")

                news_list = self.client.general_news(
                    category=self.category,
                    min_id=self.last_news_id
                )

                if not news_list:
                    await asyncio.sleep(60)
                    continue

                # normalize time
                for n in news_list:
                    n["datetime_ms"] = n["datetime"] * 1000

                # first fetch cutoff (last 24h only)
                if first_fetch:
                    newest_ts_ms = max(n["datetime_ms"] for n in news_list)
                    cutoff = newest_ts_ms - 86_400_000

                    news_list = [
                        n for n in news_list
                        if n["datetime_ms"] >= cutoff
                    ]

                    first_fetch = False

                # sort
                news_list.sort(key=lambda n: n["datetime_ms"])

                cleaned_news = []

                for news in news_list:
                    cleaned_news.append({
                        "id": news["id"],
                        "category": news.get("category"),
                        "time": news["datetime_ms"],
                        "headline": news.get("headline"),
                        "summary": news.get("summary", "")
                    })

                    if news["id"] > self.last_news_id:
                        self.last_news_id = news["id"]

                await self.writer.push(
                    "news",
                    cleaned_news,
                    self.session_pair_id
                )

                self.fail_count = 0

                await asyncio.sleep(60)

            except Exception as e:
                self.fail_count += 1

                self.logger.warning(
                    f"News error (fail={self.fail_count}): {e}"
                )

                sleep_time = min(60, 5 * self.fail_count)
                await asyncio.sleep(sleep_time + random.random())