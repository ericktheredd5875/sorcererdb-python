

# try:
#     from .core import SorcererDB
#     from .config import DBConfig
#     from .spell import Spell
# except ModuleNotFoundError as e:
#     import sys
#     print(f"[Init Warning] Could not load module: {e}", file=sys.stderr)

# from loguru import logger

from .core import SorcererDB
from .config import DBConfig
from .spell import Spell

from loguru import logger
from .logging import configure_logging


configure_logging()

