import logging
import logging.handlers
from pathlib import Path

LOG_FILE_PATH = Path("data/logs/pipeline.log")

def setup_logging():
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    if logger.hasHandlers():
        return
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")
    console_handler.setFormatter(console_formatter)

    file_handler = logging.handlers.RotatingFileHandler(
        filename = LOG_FILE_PATH, 
        mode = "a",
        maxBytes = 1 * 1024 * 1024,
        backupCount = 3,
        encoding = "utf-8"
        )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter("%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)