import asyncio, aiofiles, httpx, json
from datetime import datetime, timedelta, timezone
from utils import update_connection, get_file_path

class ContextFetcher:
    """Binance klines lekérdezés és fájlba írás (napi és 15m/30m gyertyák)."""

    def __init__(self, pair: str, session_dir: str):
        self.pair = pair
        self.session_dir = session_dir
        self.context_file = get_file_path(session_dir, pair, "ctx")

    async def run(self):
        url = "https://fapi.binance.com/fapi/v1/klines"
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    now_ts = int(datetime.utcnow().timestamp())
                    update_connection()

                    # --- Last 7 days 4h candles ---
                    start_time_4h = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)
                    params_4h = {"symbol": self.pair.upper(), "interval": "4h", "startTime": start_time_4h}
                    resp_4h = await client.get(url, params=params_4h)
                    candles_4h = resp_4h.json()
                    daily_list = [
                        {"open_time": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4], "volume": c[5]}
                        for c in candles_4h
                    ]
                    daily_record = {"period": "last_week", "symbol": self.pair, "interval": "4h",
                                    "time": now_ts*1000, "candles": daily_list}

                    # --- Last 24h 30m candles ---
                    start_time_30m = int((datetime.utcnow() - timedelta(hours=24)).timestamp() * 1000)
                    params_30m = {"symbol": self.pair.upper(), "interval": "30m", "startTime": start_time_30m}
                    resp_30m = await client.get(url, params=params_30m)
                    candles_30m = resp_30m.json()
                    intraday_list = [
                        {"open_time": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4], "volume": c[5]}
                        for c in candles_30m
                    ]
                    intraday_record = {"period": "last_day", "symbol": self.pair, "interval": "30m",
                                       "time": now_ts*1000, "candles": intraday_list}

                    async with aiofiles.open(self.context_file, "a") as f:
                        await f.write(json.dumps(daily_record) + "\n")
                        await f.write(json.dumps(intraday_record) + "\n")

                    await asyncio.sleep(3600)  # újra óránként

                except Exception as e:
                    print(f"{self.pair} context fetch error:", e)
                    await asyncio.sleep(3600)