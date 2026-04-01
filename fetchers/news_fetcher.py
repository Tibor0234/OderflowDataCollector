import asyncio, json, aiofiles
from datetime import datetime, timedelta
from utils import get_file_path

class NewsFetcher:
    def __init__(self, client, pair, session_dir, category="crypto"):
        self.client = client
        self.pair = pair
        self.category = category
        self.news_file = get_file_path(session_dir, pair, "nws")
        self.last_news_id = 0

    async def run(self):
        first_fetch = True
        while True:
            try:
                news_list = self.client.general_news(category=self.category, min_id=self.last_news_id)
                if not news_list:
                    await asyncio.sleep(60)
                    continue

                for news in news_list:
                    news["datetime_ms"] = news["datetime"] * 1000

                if first_fetch:
                    newest_ts_ms = max(n["datetime_ms"] for n in news_list)
                    cutoff = newest_ts_ms - 86_400_000
                    news_list = [n for n in news_list if n["datetime_ms"] >= cutoff]
                    first_fetch = False

                news_list = sorted(news_list, key=lambda n: n["datetime_ms"])

                async with aiofiles.open(self.news_file, "a") as f:
                    for news in news_list:
                        filtered_news = {
                            "id": news["id"],
                            "category": news["category"],
                            "symbol": self.pair.upper(),
                            "time": news["datetime_ms"],
                            "headline": news["headline"],
                            "summary": news.get("summary", "")
                        }
                        await f.write(json.dumps(filtered_news) + "\n")
                        if news["id"] > self.last_news_id:
                            self.last_news_id = news["id"]

            except Exception as e:
                print("News fetch error:", e)
            await asyncio.sleep(60)