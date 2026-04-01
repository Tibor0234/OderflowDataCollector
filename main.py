import asyncio, os
from utils import make_next_session_dir
from dotenv import load_dotenv
from fetchers.news_fetcher import NewsFetcher
from fetchers.context_fetcher import ContextFetcher
from fetchers.open_interest_fetcher import OpenInterestFetcher
from ws_clients.order_book_ws import OrderBookWS
from ws_clients.trades_ws import TradesWS
import finnhub, os

PAIRS = ["BTCUSDT"]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv()
finnhub_client = finnhub.Client(api_key=os.getenv("FINNHUB_API_KEY"))

async def main():
    tasks = []

    for pair in PAIRS:
        session_dir = make_next_session_dir(BASE_DIR, pair)

        # --- News Fetcher ---
        news_fetcher = NewsFetcher(finnhub_client, pair, session_dir)
        tasks.append(asyncio.create_task(news_fetcher.run()))

        # --- Context Fetcher ---
        context_fetcher = ContextFetcher(pair, session_dir)
        tasks.append(asyncio.create_task(context_fetcher.run()))

        # --- Open Interest Fetcher ---
        oi_fetcher = OpenInterestFetcher(pair, session_dir)
        tasks.append(asyncio.create_task(oi_fetcher.run()))

        # --- Order Book WS ---
        ob_ws = OrderBookWS(pair, session_dir)
        tasks.append(asyncio.create_task(ob_ws.run()))

        # --- Trades WS ---
        trades_ws = TradesWS(pair, session_dir)
        tasks.append(asyncio.create_task(trades_ws.run()))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())