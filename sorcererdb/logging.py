import os
import sys
from loguru import logger

__log_file = "sorcererdb.log"
__default_level = "INFO"

def configure_logging(level: str = None, log_file: str = None):
    logger.remove()

    log_level = level or os.getenv("SORCERERDB_LOG_LEVEL", __default_level)
    log_path = log_file or os.getenv("SORCERERDB_LOG_FILE", __log_file)

    #console
    logger.add(sys.stderr, level=log_level)

    # File Roatation
    logger.add(log_path, rotation="1 MB", retention="7 days", level=log_level)

    logger.debug(f"Logging configured: level={log_level}, file={log_path}")