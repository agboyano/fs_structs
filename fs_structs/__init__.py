__version__ = "0.0.1a"

from . import structs
from . import watchdog

import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.getLogger(__name__).setLevel(logging.ERROR)