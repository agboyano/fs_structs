"""fs_structs: filesystem data structures for persistent data and distributed processes.

Filesystem data structures that make persistent data management easy, let distributed
processes that only share a directory or a filesystem share data and state, and communicate
with each other. The library works on Windows and Linux machines.

- ``fs_structs.structs.FSUDict``: an unordered dict, one file per key.
- ``fs_structs.structs.FSList``: a list that can be used as a FIFO queue shared by processes.
- ``fs_structs.structs.FSNamespace``: a directory of named dicts, lists and sub-namespaces.
- ``fs_structs.structs.lock_context``: one process at a time, on any machine.
- ``fs_structs.watchdog.wait_until``: wait for a change in a directory.

Every public class and function has runnable examples in its docstring
(``help(fs_structs.structs.FSUDict)``). README.md lists the guarantees and the limits.

Examples:
    >>> import tempfile
    >>> import fs_structs
    >>> ns = fs_structs.structs.FSNamespace(tempfile.mkdtemp())
    >>> ns.udict("state")["step"] = 3
    >>> ns.state["step"]
    3
"""

__version__ = "0.0.2a0"

from . import structs
from . import watchdog

import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.getLogger(__name__).setLevel(logging.ERROR)
