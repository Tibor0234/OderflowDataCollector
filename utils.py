import os, sys, time

last_data_ts = time.time()

def update_connection(timeout: int = 120):
    global last_data_ts
    now = time.time()
    if now - last_data_ts > timeout:
        print("Connection timeout. Restarting program...")
        sys.exit(1)
    last_data_ts = now

def make_session_dir(base_dir: str, pair: str, session_num: int):
    path = os.path.join(base_dir, "data", pair.lower(), f"{pair.lower()}_session-{session_num}")
    os.makedirs(path, exist_ok=True)
    return path

def get_file_path(session_dir: str, pair: str, suffix: str):
    return os.path.join(session_dir, f"{pair}_{suffix}.txt")