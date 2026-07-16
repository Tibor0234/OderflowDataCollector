import asyncio
import os
import yaml
import finnhub

from dotenv import load_dotenv

from db.connection import (
    get_sqlite_connection,
    get_postgres_connection
)

from db.retention.sqlite_retention import SQLiteRetention
from db.retention.postgres_retention import PostgresRetention

from db.schemas.sqlite_schema import SQLiteSchema
from db.schemas.postgres_schema import PostgresSchema

from db.sessions.sqlite_session import SQLiteSession
from db.sessions.postgres_session import PostgresSession

from db.writers.sqlite_writer import SQLiteDBWriter
from db.writers.postgres_writer import PostgresDBWriter

from db.writer_queue import WriterQueue

from fetchers.news_fetcher import NewsFetcher
from fetchers.ohlcv_fetcher import OHLCVFetcher
from fetchers.open_interest_fetcher import OpenInterestFetcher

from ws_clients.order_book_ws import OrderBookWS
from ws_clients.trades_ws import TradesWS


load_dotenv()

finnhub_client = finnhub.Client(
    api_key=os.getenv("FINNHUB_API_KEY")
)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")


with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)


DB_BACKEND = config["database"]["backend"]
PAIRS = config["pairs"]
MODULES = config["modules"]
MAX_SESSIONS = config["retention"]["max_sessions"]


def setup_database():
    queue = asyncio.Queue(maxsize=50000)

    if DB_BACKEND == "sqlite":
        DB_PATH = os.getenv("SQLITE_PATH")

        conn = get_sqlite_connection(DB_PATH)

        SQLiteSchema(conn).init()

        SQLiteRetention(conn, MAX_SESSIONS).enforce()

        session = SQLiteSession(conn)
        writer = SQLiteDBWriter(conn, queue)

    elif DB_BACKEND == "postgres":
        DATABASE_URL = os.getenv("POSTGRES_URL")

        conn = get_postgres_connection(DATABASE_URL)

        PostgresSchema(conn).init()
        
        PostgresRetention(conn, MAX_SESSIONS).enforce()

        session = PostgresSession(conn)
        writer = PostgresDBWriter(conn, queue)

    else:
        raise ValueError(
            f"Unknown database backend: {DB_BACKEND}"
        )

    return conn, session, writer, WriterQueue(queue)


async def main():

    conn, session, writer, writer_queue = setup_database()

    writer_task = asyncio.create_task(
        writer.run()
    )

    tasks = []

    session_id = session.create_session()

    for pair in PAIRS:

        session_pair_id = session.create_session_pair(
            session_id,
            pair
        )


        if MODULES.get("trades"):
            tasks.append(
                asyncio.create_task(
                    TradesWS(
                        pair,
                        writer_queue,
                        session_pair_id
                    ).run()
                )
            )


        if MODULES.get("orderbook"):
            tasks.append(
                asyncio.create_task(
                    OrderBookWS(
                        pair,
                        writer_queue,
                        session_pair_id
                    ).run()
                )
            )


        if MODULES.get("open_interest"):
            tasks.append(
                asyncio.create_task(
                    OpenInterestFetcher(
                        pair,
                        writer_queue,
                        session_pair_id
                    ).run()
                )
            )


        if MODULES.get("news"):
            tasks.append(
                asyncio.create_task(
                    NewsFetcher(
                        finnhub_client,
                        pair,
                        writer_queue,
                        session_pair_id
                    ).run()
                )
            )


        if MODULES.get("ohlcv"):
            tasks.append(
                asyncio.create_task(
                    OHLCVFetcher(
                        pair,
                        writer_queue,
                        session_pair_id
                    ).run()
                )
            )


    await asyncio.gather(
        writer_task,
        *tasks
    )


if __name__ == "__main__":
    asyncio.run(main())