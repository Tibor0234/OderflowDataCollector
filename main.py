import asyncio
import os
import yaml
from dotenv import load_dotenv

import finnhub

from db.init import init_db
from db.connection import get_connection
from db.session import create_session, create_session_pair
from db.writer import DBWriter
from db.writer_queue import WriterQueue

from fetchers.news_fetcher import NewsFetcher
from fetchers.ohlcv_fetcher import OHLCVFetcher
from fetchers.open_interest_fetcher import OpenInterestFetcher
from ws_clients.order_book_ws import OrderBookWS
from ws_clients.trades_ws import TradesWS


load_dotenv()
finnhub_client = finnhub.Client(api_key=os.getenv("FINNHUB_API_KEY"))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

PAIRS = config["pairs"]
MODULES = config["modules"]

async def main():
    conn = get_connection()
    init_db(conn)

    queue = asyncio.Queue(maxsize=50000)

    writer = DBWriter(conn, queue)
    writer_task = asyncio.create_task(writer.run())

    writer_queue = WriterQueue(queue)

    tasks = []

    session_id = create_session(conn)

    for pair in PAIRS:
        session_pair_id = create_session_pair(conn, session_id, pair)

        # -------------------------
        # TRADES WS
        # -------------------------
        if MODULES.get("trades"):
            trades_ws = TradesWS(pair, writer_queue, session_pair_id)
            tasks.append(asyncio.create_task(trades_ws.run()))

        # -------------------------
        # ORDERBOOK WS
        # -------------------------
        if MODULES.get("orderbook"):
            ob_ws = OrderBookWS(pair, writer_queue, session_pair_id)
            tasks.append(asyncio.create_task(ob_ws.run()))

        # -------------------------
        # OPEN INTEREST
        # -------------------------
        if MODULES.get("open_interest"):
            oi_fetcher = OpenInterestFetcher(pair, writer_queue, session_pair_id)
            tasks.append(asyncio.create_task(oi_fetcher.run()))

        # -------------------------
        # NEWS
        # -------------------------
        if MODULES.get("news"):
            news_fetcher = NewsFetcher(
                finnhub_client,
                pair,
                writer_queue,
                session_pair_id
            )
            tasks.append(asyncio.create_task(news_fetcher.run()))

        # -------------------------
        # OHLCV
        # -------------------------
        if MODULES.get("ohlcv"):
            ohlcv_fetcher = OHLCVFetcher(pair, writer_queue, session_pair_id)
            tasks.append(asyncio.create_task(ohlcv_fetcher.run()))

    await asyncio.gather(writer_task, *tasks)


if __name__ == "__main__":
    asyncio.run(main())