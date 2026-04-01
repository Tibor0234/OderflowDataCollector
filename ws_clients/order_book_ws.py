import asyncio, websockets, json, aiofiles
from decimal import Decimal
from utils import update_connection, get_file_path

class OrderBookWS:
    def __init__(self, pair, session_dir):
        self.pair = pair
        self.session_dir = session_dir
        self.ws_url = f"wss://fstream.binance.com/ws/{pair.lower()}@depth20"
        self.ob_file = get_file_path(session_dir, pair, "ob")
        self.buffer = []

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    while True:
                        raw = await ws.recv()
                        update_connection()

                        # --- JSON dekódolás ---
                        data = json.loads(raw)

                        # --- Top 20 bids és asks ---
                        bids = data.get("b", [])
                        asks = data.get("a", [])

                        # --- Egyszerűen buffereljük a teljes snapshot-ot ---
                        record = {
                            "e": data.get("e"),
                            "E": data.get("E"),
                            "s": data.get("s"),
                            "bids": bids,
                            "asks": asks
                        }

                        self.buffer.append(json.dumps(record, default=str))

                        # --- Buffer flush fájlba minden 50 üzenet után ---
                        if len(self.buffer) >= 50:
                            async with aiofiles.open(self.ob_file, "a") as f:
                                await f.write("\n".join(self.buffer) + "\n")
                            self.buffer.clear()

            except Exception as e:
                print(f"{self.pair} OB Depth20 WS error:", e)
                if self.buffer:
                    async with aiofiles.open(self.ob_file, "a") as f:
                        await f.write("\n".join(self.buffer) + "\n")
                    self.buffer.clear()
                await asyncio.sleep(1)