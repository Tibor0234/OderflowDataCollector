import asyncio
import httpx
import aiofiles
import json
import websockets
import os
import time
import sys

# --- CONFIG ---
pairs = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]
base_dir = os.path.dirname(os.path.abspath(__file__))

def get_next_session(pair):
    pair_dir = os.path.join(base_dir, 'data', pair.lower())
    if not os.path.exists(pair_dir):
        os.makedirs(pair_dir)

    max_session = 0
    for entry in os.listdir(pair_dir):
        entry_path = os.path.join(pair_dir, entry)
        if os.path.isdir(entry_path) and entry.startswith("session_"):
            try:
                session_num = int(entry.split("_")[1])
                if session_num > max_session:
                    max_session = session_num
            except (IndexError, ValueError):
                continue

    next_session = max_session + 1
    session_dir = os.path.join(pair_dir, f"session_{next_session}")
    os.makedirs(session_dir, exist_ok=True)
    print(f"{pair.upper()} NEW SESSION STARTED -> session_{next_session}")
    return next_session

sessions = {pair.lower(): get_next_session(pair.lower()) for pair in pairs}

# --- Connection ---
last_data_ts = time.time()
SESSION_TIMEOUT = 60

def update_connection():
    global last_data_ts

    now = time.time()
    if now - last_data_ts > SESSION_TIMEOUT:
        print(f"Connection restored after timeout. Restarting program...")
        sys.exit(1)
    last_data_ts = now

# --- Függvények paraméterezve a párral ---
async def fetch_open_interest(pair):
    oi_url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={pair.upper()}"
    oi_file = os.path.join(base_dir, 'data', pair.lower(), f"session_{sessions[pair.lower()]}",
                           f"{pair.lower()}-session_{sessions[pair.lower()]}-api_oi.txt")

    async with httpx.AsyncClient() as client:
        while True:
            try:
                resp = await client.get(oi_url)
                update_connection()
                data = resp.json()
                async with aiofiles.open(oi_file, "a") as f:
                    await f.write(json.dumps(data) + "\n")
            except Exception as e:
                print(f"{pair} OI error:", e)
            await asyncio.sleep(60)

async def get_snapshot(pair):
    top, bottom = top_bottom.get(pair.lower(), (None, None))
    try:
        api_ob_url = "https://fapi.binance.com/fapi/v1/depth"
        params = {"symbol": pair.upper(), "limit": 1000}
        async with httpx.AsyncClient() as client:
            resp = await client.get(api_ob_url, params=params)
            update_connection()
            snapshot = resp.json()

        bids = [(float(p), float(q)) for p, q in snapshot["bids"]]
        asks = [(float(p), float(q)) for p, q in snapshot["asks"]]

        snap_min_bid = min(p for p, _ in bids)
        snap_max_ask = max(p for p, _ in asks)

        base_meta = {"lastUpdateId": snapshot["lastUpdateId"], "E": snapshot.get("E"), "T": snapshot.get("T")}
        ob_snapshot_file = os.path.join(base_dir, 'data', pair.lower(), f"session_{sessions[pair.lower()]}",
                                        f"{pair.lower()}-session_{sessions[pair.lower()]}-api_ob.txt")

        if top is None and bottom is None:
            bottom, top = snap_min_bid, snap_max_ask
            record = {"type": "full", **base_meta, "bids": bids, "asks": asks}
            async with aiofiles.open(ob_snapshot_file, "a") as f:
                await f.write(json.dumps(record) + "\n")
            top_bottom[pair.lower()] = (top, bottom)
            print(f"{pair} bottom: {bottom}, top: {top}")
            return

        extended = False
        if snap_min_bid < bottom:
            record = {"type": "extend", **base_meta, "bids": snapshot["bids"], "asks": []}
            bottom = snap_min_bid
            extended = True
        elif snap_max_ask > top:
            record = {"type": "extend", **base_meta, "bids": [], "asks": snapshot["asks"]}
            top = snap_max_ask
            extended = True

        if extended:
            async with aiofiles.open(ob_snapshot_file, "a") as f:
                await f.write(json.dumps(record) + "\n")
            top_bottom[pair.lower()] = (top, bottom)
            print(f"{pair} bottom: {bottom}, top: {top}")

    except Exception as e:
        print(f"{pair} snapshot error:", e)

async def order_book_ws(pair):
    ws_order_book_url = f"wss://fstream.binance.com/ws/{pair.lower()}@depth"
    ob_updates_file = os.path.join(base_dir, 'data', pair.lower(), f"session_{sessions[pair.lower()]}",
                                   f"{pair.lower()}-session_{sessions[pair.lower()]}-ws_ob.txt")
    buffer = []
    while True:
        try:
            async with websockets.connect(ws_order_book_url) as ws:
                while True:
                    ws_data_raw = await ws.recv()
                    update_connection()
                    ws_data = json.loads(ws_data_raw)

                    top, bottom = top_bottom.get(pair.lower(), (None, None))
                    if top is None or bottom is None:
                        continue

                    bids = [(float(p), float(q)) for p, q in ws_data.get("b", []) if float(p) >= bottom]
                    asks = [(float(p), float(q)) for p, q in ws_data.get("a", []) if float(p) <= top]

                    if not bids and not asks:
                        continue

                    record = ws_data.copy()
                    record["b"] = bids
                    record["a"] = asks
                    buffer.append(json.dumps(record))

                    if len(buffer) >= 50:
                        async with aiofiles.open(ob_updates_file, "a") as f:
                            await f.write("\n".join(buffer) + "\n")
                        buffer.clear()
        except Exception as e:
            if buffer:
                async with aiofiles.open(ob_updates_file, "a") as f:
                    await f.write("\n".join(buffer) + "\n")
                buffer.clear()

            print(f"{pair} OB WS error:", e)
            await asyncio.sleep(1)

async def trades_ws(pair):
    ws_trades_url = f"wss://fstream.binance.com/ws/{pair.lower()}@aggTrade"
    trades_file = os.path.join(base_dir, 'data', pair.lower(), f"session_{sessions[pair.lower()]}",
                               f"{pair.lower()}-session_{sessions[pair.lower()]}-ws_tr.txt")
    buffer = []
    while True:
        try:
            async with websockets.connect(ws_trades_url) as ws:
                while True:
                    ws_data_raw = await ws.recv()
                    update_connection()
                    ws_data = json.loads(ws_data_raw)
                    price, qty = float(ws_data['p']), float(ws_data['q'])
                    if price == 0 or qty == 0:
                        continue

                    top, bottom = top_bottom.get(pair.lower(), (None, None))
                    if top is None or bottom is None:
                        continue

                    if price > top or price < bottom:
                        print(f"{pair} window broken:", price)
                        await get_snapshot(pair)

                    buffer.append(ws_data_raw)
                    if len(buffer) >= 50:
                        async with aiofiles.open(trades_file, "a") as f:
                            await f.write("\n".join(buffer) + "\n")
                        buffer.clear()
        except Exception as e:
            if len(buffer) >= 50:
                async with aiofiles.open(trades_file, "a") as f:
                    await f.write("\n".join(buffer) + "\n")
                buffer.clear()

            print(f"{pair} Trades WS error:", e)
            await asyncio.sleep(1)

# --- MAIN ---
top_bottom = {}

async def main():
    tasks = []
    snapshot_tasks = [get_snapshot(pair) for pair in pairs]
    await asyncio.gather(*snapshot_tasks)

    for pair in pairs:
        tasks.append(asyncio.create_task(fetch_open_interest(pair)))
        tasks.append(asyncio.create_task(order_book_ws(pair)))
        tasks.append(asyncio.create_task(trades_ws(pair)))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())