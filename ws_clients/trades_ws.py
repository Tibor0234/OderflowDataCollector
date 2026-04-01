import asyncio, websockets, json, aiofiles
from decimal import Decimal
from utils import update_connection, get_file_path

class TradesWS:
    def __init__(self, pair, session_dir):
        self.pair = pair
        self.session_dir = session_dir
        self.ws_url = f"wss://fstream.binance.com/ws/{pair.lower()}@aggTrade"
        self.trades_file = get_file_path(session_dir, pair, "tr")
        self.buffer = []

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    while True:
                        raw = await ws.recv()
                        update_connection()
                        data = json.loads(raw)
                        price, qty = Decimal(data['p']), Decimal(data['q'])
                        if price == 0 or qty == 0:
                            continue

                        self.buffer.append(raw)
                        if len(self.buffer) >= 50:
                            async with aiofiles.open(self.trades_file, "a") as f:
                                await f.write("\n".join(self.buffer) + "\n")
                            self.buffer.clear()
            except Exception as e:
                print(f"{self.pair} Trades WS error:", e)
                await asyncio.sleep(1)