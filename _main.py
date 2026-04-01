import asyncio
import httpx
import aiofiles
import json
import websockets
import os
import time
import sys
from decimal import Decimal
from dotenv import load_dotenv
import finnhub
from datetime import datetime, timedelta, timezone

# ---------------------------
# --- CONFIG / SETTINGS ---
# ---------------------------
PAIRS = ["BTCUSDT"]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_TIMEOUT = 60            # másodperc
ORDER_BOOK_DEPTH = 1000         # max 1000
MARKET_NEWS_CATEGORY = "crypto"

top_bottom = {}                 # {pair: (top, bottom)}
last_data_ts = time.time()

load_dotenv()
finnhub_client = finnhub.Client(api_key=os.getenv("FINNHUB_API_KEY"))


# ---------------------------
# --- SESSION MANAGEMENT ---
# ---------------------------
def get_next_session(pair: str) -> int:
    """Létrehoz új session mappát és visszaadja a session számot."""
    pair_dir = os.path.join(BASE_DIR, "data", pair.lower())
    os.makedirs(pair_dir, exist_ok=True)

    max_session = 0
    for entry in os.listdir(pair_dir):
        entry_path = os.path.join(pair_dir, entry)
        if os.path.isdir(entry_path) and entry.startswith(f"{pair.lower()}_session-"):
            try:
                session_num = int(entry.split("-")[1])
                max_session = max(max_session, session_num)
            except (IndexError, ValueError):
                continue

    next_session = max_session + 1
    session_dir = os.path.join(pair_dir, f"{pair.lower()}_session-{next_session}")
    os.makedirs(session_dir, exist_ok=True)
    print(f"NEW SESSION STARTED -> {pair.lower()}_session-{next_session}")
    return next_session

SESSIONS = {pair.lower(): get_next_session(pair.lower()) for pair in PAIRS}


# ---------------------------
# --- CONNECTION WATCHDOG ---
# ---------------------------
def update_connection():
    """Frissíti a timestamp-et, kilép ha timeout."""
    global last_data_ts
    now = time.time()
    if now - last_data_ts > SESSION_TIMEOUT:
        print("Connection restored after timeout. Restarting program...")
        sys.exit(1)
    last_data_ts = now


# ---------------------------
# --- HELPERS ---
# ---------------------------
def get_session_dir(pair: str) -> str:
    """Visszaadja a session mappa útvonalát a pair-hez."""
    return os.path.join(BASE_DIR, "data", pair.lower(), f"{pair.lower()}_session-{SESSIONS[pair.lower()]}")


def get_file_path(pair: str, suffix: str) -> str:
    """Összeállítja az adott pair file nevét a sessionben."""
    session_dir = get_session_dir(pair)
    return os.path.join(session_dir, f"{pair.lower()}_session-{SESSIONS[pair.lower()]}_{suffix}.txt")


# ---------------------------
# --- NEWS FETCHER ---
# ---------------------------
async def fetch_news(pair: str):
    """Finnhub news pollolása és fájlba írása (induláskor csak az utolsó 24h)."""
    last_news_id = 0
    news_file = get_file_path(pair, "nws")

    first_fetch = True

    while True:
        try:
            news_list = finnhub_client.general_news(
                category=MARKET_NEWS_CATEGORY,
                min_id=last_news_id
            )

            if not news_list:
                await asyncio.sleep(60)
                continue

            for news in news_list:
                news["datetime_ms"] = news["datetime"] * 1000

            if first_fetch:
                newest_ts_ms = max(n["datetime_ms"] for n in news_list)
                cutoff = newest_ts_ms - 86_400_000  # 24h
                news_list = [
                    n for n in news_list
                    if (n["datetime_ms"]) >= cutoff
                ]
                first_fetch = False

            news_list = sorted(news_list, key=lambda n: n["datetime_ms"])

            for news in news_list:
                filtered_news = {
                    "id": news["id"],
                    "category": news["category"],
                    "symbol": pair.upper(),
                    "time": news["datetime_ms"],
                    "headline": news["headline"],
                    "summary": news.get("summary", "")
                }

                async with aiofiles.open(news_file, "a") as f:
                    await f.write(json.dumps(filtered_news) + "\n")

                if news["id"] > last_news_id:
                    last_news_id = news["id"]

        except Exception as e:
            print("Finnhub news fetch error:", e)

        await asyncio.sleep(60)


# ---------------------------
# --- CONTEXT FETCHER ---
# ---------------------------
async def fetch_context(pair: str):
    """Binance klines pollolása és fájlba írás (napi és 15m gyertyák)."""
    
    context_file = get_file_path(pair, "ctx")
    url = "https://fapi.binance.com/fapi/v1/klines"
    
    async with httpx.AsyncClient() as client:
        try:
            now_dt = datetime.fromtimestamp(last_data_ts, tz=timezone.utc)

            # --- 2. Utolsó 1w 4h gyertyák ---
            start_time = int((now_dt - timedelta(days=7)).timestamp() * 1000)
            params_daily = {"symbol": pair.upper(), "interval": "4h", "startTime": start_time}
            resp_daily = await client.get(url, params=params_daily)
            daily_candles = resp_daily.json()

            daily_list = [
                {
                    "open_time": c[0],
                    "open": c[1],
                    "high": c[2],
                    "low": c[3],
                    "close": c[4],
                    "volume": c[5],
                }
                for c in daily_candles
            ]
            daily_record = {
                "period": "last_week",
                "symbol": pair,
                "interval": "4h",
                "time": last_data_ts * 1000,
                "candles": daily_list
                }

            # --- 2. Utolsó 24h 15m gyertyák ---
            
            start_time = int((now_dt - timedelta(hours=24)).timestamp() * 1000)
            params_15m = {"symbol": pair.upper(), "interval": "30m", "startTime": start_time}
            resp_15m = await client.get(url, params=params_15m)
            intraday_candles = resp_15m.json()

            intraday_list = [
                {
                    "open_time": c[0],
                    "open": c[1],
                    "high": c[2],
                    "low": c[3],
                    "close": c[4],
                    "volume": c[5],
                }
                for c in intraday_candles
            ]
            intraday_record = {
                "period": 'last_day',
                "symbol": pair,
                "interval": "30m",
                "time": last_data_ts * 1000,
                "candles": intraday_list
                }

            async with aiofiles.open(context_file, "a") as f:
                await f.write(json.dumps(daily_record) + "\n")
                await f.write(json.dumps(intraday_record) + "\n")

        except Exception as e:
            print(f"{pair} context fetch error:", e)
            
            await asyncio.sleep(86_400)

# ---------------------------
# --- API FETCHERS ---
# ---------------------------
async def fetch_open_interest(pair: str):
    """Binance Open Interest API pollolása és file-írás."""
    oi_file = get_file_path(pair, "api-oi")
    url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={pair.upper()}"
    async with httpx.AsyncClient() as client:
        while True:
            try:
                resp = await client.get(url)
                update_connection()
                data = resp.json()
                async with aiofiles.open(oi_file, "a") as f:
                    await f.write(json.dumps(data) + "\n")
            except Exception as e:
                print(f"{pair} OI error:", e)
            await asyncio.sleep(60)


async def get_snapshot(pair: str):
    """Binance Order Book snapshot és top/bottom tracking."""
    top, bottom = top_bottom.get(pair.lower(), (None, None))
    ob_file = get_file_path(pair, "api-ob")
    url = "https://fapi.binance.com/fapi/v1/depth"
    params = {"symbol": pair.upper(), "limit": ORDER_BOOK_DEPTH}

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params=params)
            update_connection()
            snapshot = resp.json()
        except Exception as e:
            print(f"{pair} snapshot API error:", e)
            return

    bids = [(Decimal(p), Decimal(q)) for p, q in snapshot["bids"]]
    asks = [(Decimal(p), Decimal(q)) for p, q in snapshot["asks"]]

    snap_min_bid = min(p for p, _ in bids)
    snap_max_ask = max(p for p, _ in asks)
    base_meta = {"lastUpdateId": snapshot["lastUpdateId"], "E": snapshot.get("E"), "T": snapshot.get("T")}

    # Inicializálás
    if top is None or bottom is None:
        top_bottom[pair.lower()] = (snap_max_ask, snap_min_bid)
        record = {"symbol": pair, **base_meta, "bids": bids, "asks": asks}
        async with aiofiles.open(ob_file, "a") as f:
            await f.write(json.dumps(record, default=str) + "\n")
        return

    extended = False
    if snap_min_bid < bottom:
        record = {"symbol": pair, **base_meta, "bids": snapshot["bids"], "asks": []}
        bottom = snap_min_bid
        extended = True
    if snap_max_ask > top:
        record = {"symbol": pair, **base_meta, "bids": [], "asks": snapshot["asks"]}
        top = snap_max_ask
        extended = True

    if extended:
        async with aiofiles.open(ob_file, "a") as f:
            await f.write(json.dumps(record, default=str) + "\n")
        top_bottom[pair.lower()] = (top, bottom)


# ---------------------------
# --- WEBSOCKET HANDLERS ---
# ---------------------------
async def order_book_ws(pair: str):
    """Binance order book websocket, window filter + file buffer."""
    url = f"wss://fstream.binance.com/ws/{pair.lower()}@depth"
    ob_file = get_file_path(pair, "ws-ob")
    buffer = []

    while True:
        try:
            async with websockets.connect(url) as ws:
                while True:
                    raw = await ws.recv()
                    update_connection()
                    data = json.loads(raw)
                    top, bottom = top_bottom.get(pair.lower(), (None, None))
                    if top is None or bottom is None:
                        continue

                    bids = [(Decimal(p), Decimal(q)) for p, q in data.get("b", []) if Decimal(p) >= bottom].reverse()
                    asks = [(Decimal(p), Decimal(q)) for p, q in data.get("a", []) if Decimal(p) <= top]

                    if not bids and not asks:
                        continue

                    record = data.copy()
                    record["b"] = bids
                    record["a"] = asks
                    buffer.append(json.dumps(record, default=str))

                    if len(buffer) >= 50:
                        async with aiofiles.open(ob_file, "a") as f:
                            await f.write("\n".join(buffer) + "\n")
                        buffer.clear()
        except Exception as e:
            if buffer:
                async with aiofiles.open(ob_file, "a") as f:
                    await f.write("\n".join(buffer) + "\n")
                buffer.clear()
            print(f"{pair} OB WS error:", e)
            await asyncio.sleep(1)


async def trades_ws(pair: str):
    """Binance aggTrades websocket, price check + file buffer."""
    url = f"wss://fstream.binance.com/ws/{pair.lower()}@aggTrade"
    trades_file = get_file_path(pair, "ws-tr")
    buffer = []

    while True:
        try:
            async with websockets.connect(url) as ws:
                while True:
                    raw = await ws.recv()
                    update_connection()
                    data = json.loads(raw)
                    price, qty = Decimal(data['p']), Decimal(data['q'])
                    if price == 0 or qty == 0:
                        continue

                    top, bottom = top_bottom.get(pair.lower(), (None, None))
                    if top is None or bottom is None:
                        continue

                    if price > top or price < bottom:
                        print(f"{pair} window broken: {price}")
                        await get_snapshot(pair)

                    buffer.append(raw)
                    if len(buffer) >= 50:
                        async with aiofiles.open(trades_file, "a") as f:
                            await f.write("\n".join(buffer) + "\n")
                        buffer.clear()
        except Exception as e:
            if buffer:
                async with aiofiles.open(trades_file, "a") as f:
                    await f.write("\n".join(buffer) + "\n")
                buffer.clear()
            print(f"{pair} Trades WS error:", e)
            await asyncio.sleep(1)


# ---------------------------
# --- MAIN LOOP ---
# ---------------------------
async def main():
    # snapshot inicializálás minden pair-hez
    await asyncio.gather(*(get_snapshot(pair) for pair in PAIRS))

    # websocket + API polling
    tasks = []
    for pair in PAIRS:
        tasks.extend([
            asyncio.create_task(fetch_context(pair)),
            asyncio.create_task(fetch_news(pair)),
            asyncio.create_task(fetch_open_interest(pair)),
            asyncio.create_task(order_book_ws(pair)),
            asyncio.create_task(trades_ws(pair))
        ])
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())