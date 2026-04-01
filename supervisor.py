import subprocess
import time
import os
import signal
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_SCRIPT = os.path.join(BASE_DIR, "main.py")
EXIT_RESTART_CODE = 1
LOG_FILE = os.path.join(BASE_DIR, "log.txt")

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}\n"
    print(line, end="")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line)

def run_main():
    proc = None
    try:
        while True:
            log("➡️ Indítjuk a main.py-t...")
            proc = subprocess.Popen(["python", MAIN_SCRIPT])
            proc.wait()
            exit_code = proc.returncode
            log(f"main.py leállt exit code: {exit_code}")

            if exit_code == EXIT_RESTART_CODE:
                log("🔁 Újraindítjuk a main.py-t")
                time.sleep(2)
                continue
            else:
                log("✅ Kilépünk a processhandlerből\n")
                break
    except KeyboardInterrupt:
        log("🛑 processhandler megszakítva, leállítjuk a main.py-t is")
        if proc and proc.poll() is None:
            proc.send_signal(signal.SIGINT)  # Ctrl+C jelet küld
            proc.wait()
        log("✅ main.py leállítva\n")

if __name__ == "__main__":
    run_main()