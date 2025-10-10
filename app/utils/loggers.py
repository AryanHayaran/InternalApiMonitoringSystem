import logging
import logging.handlers
import sys
from pathlib import Path

# ----------------------------
# Config
# ----------------------------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)  # create log directory if not exists
LOG_FILE = LOG_DIR / "app.log"
LOG_LEVEL = logging.INFO  # Change to DEBUG for dev, INFO/ERROR for prod

# ----------------------------
# Formatter
# ----------------------------
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ----------------------------
# Console Handler (Real-Time Logs)
# ----------------------------
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
console_handler.setLevel(LOG_LEVEL)

# ----------------------------
# File Handler with Rotation
# ----------------------------
file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=5,  # keep last 5 log files
    encoding="utf-8"
)
file_handler.setFormatter(formatter)
file_handler.setLevel(LOG_LEVEL)

# ----------------------------
# Logger Setup
# ----------------------------
logger = logging.getLogger("app")  # root logger for your app
logger.setLevel(LOG_LEVEL)
logger.addHandler(console_handler)  # real-time logs
logger.addHandler(file_handler)     # rotating file logs
logger.propagate = False  # prevent double logging if used in FastAPI/Uvicorn

# ----------------------------
# Helper Function
# ----------------------------
def get_logger(name: str = "app") -> logging.Logger:
    """Get a logger instance for any module."""
    return logging.getLogger(name)
