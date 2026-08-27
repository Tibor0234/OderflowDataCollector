import sqlite3
import asyncio

from db.schemas.sqlite_schema import SQLiteSchema
from db.sessions.sqlite_session import SQLiteSession
from db.writers.sqlite_writer import SQLiteDBWriter


def test_session_pair_persists_instrument_metadata():
    conn = sqlite3.connect(":memory:")
    SQLiteSchema(conn).init()
    session = SQLiteSession(conn)

    session_id = session.create_session()
    session_pair_id = session.create_session_pair(
        session_id,
        "BTCUSDT",
        {
            "contract_type": "PERPETUAL",
            "status": "TRADING",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "tick_size": "0.10",
            "quantity_step": "0.001",
            "price_precision": 2,
            "quantity_precision": 3,
            "min_quantity": "0.001",
            "min_notional": "5",
            "onboard_date": "2019-09-08T00:00:00"
        }
    )

    row = conn.execute(
        """
        SELECT session_pair_id, symbol, contract_type, status, base_asset, quote_asset,
             tick_size, quantity_step, price_precision, quantity_precision,
             min_quantity, min_notional, onboard_date
        FROM instrument_metadata
        """
    ).fetchone()

    assert row == (
        session_pair_id,
        "btc usdt".replace(" ", ""),
        "PERPETUAL",
        "TRADING",
        "BTC",
        "USDT",
        0.1,
        0.001,
        2,
        3,
        0.001,
        5,
        "2019-09-08T00:00:00"
    )


def test_ohlcv_candles_share_fetch_timestamp_and_foreign_key():
    conn = sqlite3.connect(":memory:")
    SQLiteSchema(conn).init()
    session = SQLiteSession(conn)
    session_id = session.create_session()
    session_pair_id = session.create_session_pair(
        session_id,
        "BTCUSDT",
        {
            "contract_type": "PERPETUAL",
            "status": "TRADING",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "tick_size": "0.10"
        }
    )

    writer = SQLiteDBWriter(conn, asyncio.Queue())
    writer._insert_ohlcv({
        "session_pair_id": session_pair_id,
        "interval": "30m",
        "period": "last_day",
        "timestamp": "2026-08-27T12:00:00+00:00",
        "candles": [
            {"open_time": 1000, "open": "1", "high": "2", "low": "1", "close": "2", "volume": "3"},
            {"open_time": 2000, "open": "2", "high": "3", "low": "2", "close": "3", "volume": "4"}
        ]
    })

    rows = conn.execute(
        """
        SELECT fetch.session_pair_id, fetch.timestamp, candle.fetch_id
        FROM ohlcv_fetches AS fetch
        JOIN ohlcv AS candle ON candle.fetch_id = fetch.id
        ORDER BY candle.open_time
        """
    ).fetchall()

    assert rows == [
        (session_pair_id, "2026-08-27T12:00:00+00:00", rows[0][2]),
        (session_pair_id, "2026-08-27T12:00:00+00:00", rows[1][2])
    ]
    assert rows[0][2] == rows[1][2]


def test_session_is_reused_for_the_same_day():
    conn = sqlite3.connect(":memory:")
    SQLiteSchema(conn).init()
    session = SQLiteSession(conn)

    first_session_id = session.get_or_create_session()
    second_session_id = session.get_or_create_session()

    assert second_session_id == first_session_id
    assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 1


def test_session_pair_is_reused_with_existing_metadata():
    conn = sqlite3.connect(":memory:")
    SQLiteSchema(conn).init()
    session = SQLiteSession(conn)
    session_id = session.get_or_create_session()
    metadata = {
        "contract_type": "PERPETUAL",
        "status": "TRADING",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "tick_size": "0.10"
    }

    first_pair_id = session.get_or_create_session_pair(
        session_id, "BTCUSDT", metadata
    )
    second_pair_id = session.get_or_create_session_pair(
        session_id, "BTCUSDT", {**metadata, "status": "BREAK"}
    )

    assert second_pair_id == first_pair_id
    assert conn.execute("SELECT COUNT(*) FROM session_pairs").fetchone()[0] == 1
    assert conn.execute(
        "SELECT status FROM instrument_metadata"
    ).fetchone()[0] == "TRADING"