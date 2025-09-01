from loguru import logger
from result import Ok, Err, Result, is_ok, is_err

logger.add(
    "logs/extraction.log", 
    rotation="1 MB",
    retention="7 days",
    compression="zip",
    level="DEBUG",
    format="[{time:YYYY-MM-DD HH:mm:ss}] [{file}:{line}] [{level}] : {message}"
)