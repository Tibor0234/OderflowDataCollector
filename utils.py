import os, sys, time, re

last_data_ts = time.time()

def update_connection(timeout: int = 120):
    global last_data_ts
    now = time.time()
    if now - last_data_ts > timeout:
        print("Connection timeout. Restarting program...")
        sys.exit(1)
    last_data_ts = now

def make_next_session_dir(base_dir: str, pair: str):
    pair = pair.lower()
    pair_dir = os.path.join(base_dir, "data", pair)
    
    # Ha a pair mappa nem létezik, létrehozzuk
    os.makedirs(pair_dir, exist_ok=True)
    
    # Regex a session mappákhoz
    session_pattern = re.compile(rf"{pair}_session-(\d+)")
    max_session = 0
    
    # Csak a max számot keressük
    for name in os.listdir(pair_dir):
        match = session_pattern.fullmatch(name)
        if match:
            session_num = int(match.group(1))
            if session_num > max_session:
                max_session = session_num
    
    next_session = max_session + 1
    new_session_path = os.path.join(pair_dir, f"{pair}_session-{next_session}")
    os.makedirs(new_session_path, exist_ok=True)
    
    print(f"News session started: {pair.upper()} - session {next_session}")
    return new_session_path

def get_file_path(session_dir: str, pair: str, suffix: str):
    return os.path.join(session_dir, f"{pair}_{suffix}.txt")