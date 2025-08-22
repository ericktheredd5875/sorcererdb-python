# try:
#     from .core import SorcererDB
#     from .config import DBConfig
#     from .spell import Spell
# except ModuleNotFoundError as e:
#     import sys
#     print(f"[Init Warning] Could not load module: {e}", file=sys.stderr)

# from loguru import logger

from loguru import logger

from .config import DBConfig
from .errors import *
from .logging import configure_logging
from .sorcerer import Sorcerer
from .spell import Spell

__all__ = [
    "Sorcerer",
    "DBConfig",
    "Spell",
    "configure_logging",
]

configure_logging()
