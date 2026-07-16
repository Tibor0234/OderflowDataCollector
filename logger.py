import logging
import os
from datetime import datetime


class LoggerManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)

            cls._instance._initialized = False
            cls._instance.log_file = None
            cls._instance.log_dir = "logs"

        return cls._instance

    def initialize(self, log_dir="logs"):
        if self._initialized:
            return

        self.log_dir = log_dir

        os.makedirs(self.log_dir, exist_ok=True)

        self.log_file = (
            f"{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        )

        log_path = os.path.join(
            self.log_dir,
            self.log_file
        )

        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        file_handler = logging.FileHandler(
            log_path,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)

        # védelem duplikált handlerek ellen
        root_logger.handlers.clear()

        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

        # zajos külső libraryk
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)

        self._initialized = True

    def get_logger(self, name):
        if not self._initialized:
            self.initialize()

        return logging.getLogger(name)

    def get_log_file(self):
        return self.log_file

    def get_log_path(self):
        if self.log_file is None:
            return None

        return os.path.join(
            self.log_dir,
            self.log_file
        )