import os
import sys
from pathlib import Path
from loguru import logger

__log_dir = Path(os.getenv("SORCERERDB_LOG_DIR", Path.cwd()/"logs"))
__log_file = __log_dir / "sorcererdb.log"
__default_level = "INFO"

def configure_logging(level: str = None, log_file: str = None):
    logger.remove()

    log_level = level or os.getenv("SORCERERDB_LOG_LEVEL", __default_level)
    log_path  = log_file or os.getenv("SORCERERDB_LOG_FILE", __log_file)

    log_dir = Path(log_path).parent
    log_dir.mkdir(parents=True, exist_ok=True)

    #console
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>"
    )

    # 📁 File handler
    logger.add(
        sink        = str(log_path),
        level       = log_level,
        rotation    = "1 MB",
        retention   = "7 days",
        compression = "zip",
        enqueue     = True,
        backtrace   = True,
        diagnose    = True
    )
