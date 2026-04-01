import asyncio, aiofiles, httpx, json
from utils import get_file_path

class OpenInterestFetcher:
    """Binance Open Interest API lekérdezés és fájlba írás."""

    def __init__(self, pair: str, session_dir: str):
        self.pair = pair
        self.session_dir = session_dir
        self.oi_file = get_file_path(session_dir, pair, "oi")
        self.url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={pair.upper()}"

    async def run(self):
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    resp = await client.get(self.url)
                    data = resp.json()
                    async with aiofiles.open(self.oi_file, "a") as f:
                        await f.write(json.dumps(data) + "\n")
                except Exception as e:
                    print(f"{self.pair} Open Interest error:", e)
                await asyncio.sleep(60)