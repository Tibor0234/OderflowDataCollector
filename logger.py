import os
import logging
from datetime import datetime


_handlers_initialized = False

def setup_logger(name, log_dir="logs"):
    global _handlers_initialized

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not _handlers_initialized:

        log_file = os.path.join(
            log_dir,
            f"{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        )

        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(formatter)

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)

        root = logging.getLogger()
        root.setLevel(logging.INFO)

        root.addHandler(fh)
        root.addHandler(ch)

        _handlers_initialized = True

    return logger