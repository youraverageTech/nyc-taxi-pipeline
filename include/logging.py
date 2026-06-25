import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
log_file = os.path.join(LOG_DIR, "logger.log")
logger_name = "nyc_taxi_pipeline"
TIMEZONE = ZoneInfo("Asia/Jakarta")

class JakartaFormatter(logging.Formatter):
    """Custom formatter that renders timestamps in Asia/Jakarta timezone."""

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=TIMEZONE)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

# Setting up logging
def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger(logger_name)

    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    formatter = JakartaFormatter(
        fmt="%(asctime)s | %(levelname)-8s | %(module)-10s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)

    return logger

# Log to use for the module
def get_logger():
    return logging.getLogger(logger_name)