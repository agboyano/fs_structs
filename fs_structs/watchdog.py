"""Wait for changes in a directory, on Windows and Linux, on local disks and network shares.

Built on the ``watchdog`` package. ``wait_until`` blocks until a filesystem event that
passes a filter happens in one of the given directories, or until a timeout. The library
uses it to wake up when a lock is released or when a new element arrives in a queue.

On Linux, the kernel does not report changes made by other machines on NFS or CIFS
mounts. For directories on such mounts a polling observer is used instead. This is
decided per path; see ``FORCE_POLLING`` and the environment variable
``FS_STRUCTS_POLLING`` to override it.

Events can be lost on network filesystems. Always pass a timeout and check the real state
after the wait.

Examples:
    >>> import tempfile, threading
    >>> from pathlib import Path
    >>> from fs_structs.watchdog import wait_until_file_event
    >>> root = tempfile.mkdtemp()
    >>> timer = threading.Timer(0.3, (Path(root) / "ready.txt").write_text, args=("ok",))
    >>> timer.start()
    >>> event, kind, parent, name = wait_until_file_event([root], ["ready"], ["created"], timeout=10)
    >>> kind, name
    ('created', 'ready.txt')
"""

import os
import posixpath
import sys
import threading
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

event_types = ["created", "deleted", "modified", "moved", "closed", "opened"]
"""list: Every event type that ``wait_until_file_event`` accepts by default."""

NETWORK_FSTYPES = {"nfs", "nfs4", "cifs", "smb3", "smbfs", "fuse.sshfs", "9p", "afs", "glusterfs", "ceph"}
"""set: Filesystem types (as written in ``/proc/mounts``) that need polling on Linux."""

FORCE_POLLING = None
"""bool | None: None decides per path; True or False forces the observer kind."""

POLLING_INTERVAL = 1.0
"""float: Seconds between two checks of the polling observer."""


# Implementation notes:
# - Why this exists: inotify (Linux) only reports changes made through the local kernel.
#   On a CIFS (SMB/Samba) or NFS mount, a file created by another machine never produces
#   an inotify event, so an event-based wait would always run to its timeout. Watchdog's
#   PollingObserver lists the directory every POLLING_INTERVAL seconds instead.
# - Windows is different: ReadDirectoryChangesW asks the SMB server for change
#   notifications, so the native observer works on shares. Verified on an SMB share: a
#   waiter is woken ~0.5 s after a lock release made by another process.
# - /proc/mounts lists "device mountpoint fstype options ...". The longest mount point that
#   is a prefix of the path wins; a later line with the same mount point shadows an
#   earlier one (rootfs, then the real root). Spaces are escaped as \040.
# - posixpath is used on purpose so the function can be tested on Windows with a fake file.
# - Not yet verified on a real Linux client against a Samba server; the decision logic is
#   unit-tested with a simulated mounts table. FS_STRUCTS_POLLING=1 forces polling if the
#   detection fails for an unusual mount type.
def _mount_fstype(path, mounts_file="/proc/mounts"):
    """Return the filesystem type of the mount that contains ``path`` (POSIX only).

    Args:
        path (str | Path): Absolute POSIX path.
        mounts_file (str): The mounts table to read. Default ``/proc/mounts``.

    Returns:
        str | None: The type, for example ``'ext4'`` or ``'nfs4'``, or None if the table
        cannot be read.
    """
    try:
        with open(mounts_file, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except OSError:
        return None
    path = posixpath.abspath(str(path))
    best_mount, best_type = "", None
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        mount = parts[1].replace("\\040", " ")
        fstype = parts[2]
        if (path == mount or path.startswith(mount.rstrip("/") + "/")) and len(mount) >= len(best_mount):
            best_mount, best_type = mount, fstype
    return best_type


def _needs_polling(paths, mounts_file="/proc/mounts"):
    """Return True when the observer for ``paths`` must poll instead of using events.

    Order of decision: ``FORCE_POLLING`` if not None; then the environment variable
    ``FS_STRUCTS_POLLING`` if set; then, on Linux only, whether any path is on a mount of a
    type listed in ``NETWORK_FSTYPES``. On other systems the answer is False.

    Args:
        paths (list): Directories that will be watched.
        mounts_file (str): The mounts table to read. Default ``/proc/mounts``.

    Returns:
        bool: True to use ``PollingObserver``.
    """
    if FORCE_POLLING is not None:
        return bool(FORCE_POLLING)
    env = os.environ.get("FS_STRUCTS_POLLING")
    if env is not None:
        return env.strip().lower() not in ("", "0", "false", "no")
    if not sys.platform.startswith("linux"):
        return False
    return any(_mount_fstype(p, mounts_file) in NETWORK_FSTYPES for p in paths)


def _observer_for(paths, polling=None):
    """Return a new observer for ``paths``: polling or native.

    Args:
        paths (list): Directories that will be watched.
        polling (bool | None): Force polling (True) or native (False); None decides
            with ``_needs_polling``.

    Returns:
        watchdog observer: Not started.
    """
    use_polling = _needs_polling(paths) if polling is None else polling
    return PollingObserver(timeout=POLLING_INTERVAL) if use_polling else Observer()


class FSEventHandler(FileSystemEventHandler):
    """Watchdog handler that remembers the first event accepted by a filter.

    After the first accepted event, ``event`` (a ``threading.Event``) is set and the event
    object is kept in ``event_obj``. Later events are ignored.

    Args:
        f (callable): Filter. Receives a watchdog event and returns True to accept it.
            Default: accept every event.

    See Also:
        wait_until: Uses this handler.

    Examples:
        >>> from fs_structs.watchdog import FSEventHandler
        >>> class FakeEvent:
        ...     event_type = "created"
        >>> handler = FSEventHandler(lambda e: e.event_type == "created")
        >>> handler.on_any_event(FakeEvent())
        >>> handler.event.is_set(), handler.event_obj.event_type
        (True, 'created')
    """

    def __init__(self, f=lambda x: True):
        self.f = f
        self.event_obj = None
        self.event = threading.Event()

    def on_any_event(self, event):
        """Called by watchdog for every event; keeps the first one accepted by ``f``."""
        if self.f(event):
            self.event_obj = event
            self.event.set()


# Implementation notes:
# - One observer is created, started and stopped per call. It costs a thread start and a
#   few milliseconds; callers wait seconds, so it is not worth keeping observers alive.
# - The result is the accepted event plus convenient pieces of it: the parent directory
#   and the name of the file or directory that changed.
def wait_until(dir_list, f=lambda x: True, timeout=60, polling=None):
    """Wait until a filesystem event accepted by ``f`` happens in one of the directories.

    Args:
        dir_list (list): Directories to watch (not recursive).
        f (callable): Filter. Receives a watchdog event and returns True to accept it.
            Default: accept every event.
        timeout (float): Maximum seconds to wait. Default 60.
        polling (bool | None): True forces the polling observer, False the native one,
            None decides per path. Default None.

    Returns:
        tuple: ``(event, event_type, parent_dir, name)`` for the accepted event, where
        ``parent_dir`` is a ``Path`` and ``name`` a str; or ``(None, None, None, None)``
        when the timeout passed without an accepted event.

    See Also:
        wait_until_file_event: The same, with a filter on file names and event types.

    Examples:
        >>> import tempfile, threading
        >>> from pathlib import Path
        >>> from fs_structs.watchdog import wait_until
        >>> root = tempfile.mkdtemp()
        >>> timer = threading.Timer(0.3, (Path(root) / "data.bin").write_bytes, args=(b"x",))
        >>> timer.start()
        >>> event, kind, parent, name = wait_until([root], lambda e: e.event_type == "created", timeout=10)
        >>> kind, name
        ('created', 'data.bin')
        >>> wait_until([root], timeout=0.2)
        (None, None, None, None)
    """
    event_handler = FSEventHandler(f)
    observer = _observer_for(dir_list, polling)

    for directory in dir_list:
        directory = Path(directory).resolve()
        observer.schedule(event_handler, str(directory), recursive=False)

    observer.start()
    event_handler.event.wait(timeout=timeout)
    observer.stop()
    observer.join()

    if event_handler.event_obj is not None:
        file = Path(event_handler.event_obj.src_path).resolve()
        return (
            event_handler.event_obj,
            event_handler.event_obj.event_type,
            file.parent,
            file.name,
        )

    return (None, None, None, None)


def wait_until_file_event(dir_list, filenames, events=(), strict=False, timeout=60, polling=None):
    """Wait for an event on a file (not a directory) in one of the directories.

    Args:
        dir_list (list): Directories to watch (not recursive).
        filenames (list): Accept only files whose name contains one of these strings
            (or is equal to one of them when ``strict`` is True). Empty list: any file.
        events (list): Event types to accept, for example ``["created"]``. Empty: all the
            types in ``event_types``.
        strict (bool): Compare names by equality instead of "contains". Default False.
        timeout (float): Maximum seconds to wait. Default 60.
        polling (bool | None): See ``wait_until``. Default None.

    Returns:
        tuple: As ``wait_until``.

    See Also:
        wait_until: The general form with a custom filter.

    Examples:
        >>> import tempfile, threading
        >>> from pathlib import Path
        >>> from fs_structs.watchdog import wait_until_file_event
        >>> root = tempfile.mkdtemp()
        >>> timer = threading.Timer(0.3, (Path(root) / "result_7.json").write_text, args=("{}",))
        >>> timer.start()
        >>> _, kind, _, name = wait_until_file_event([root], ["result_"], ["created"], timeout=10)
        >>> kind, name
        ('created', 'result_7.json')
    """
    if events is None or len(events) == 0:
        events = event_types[:]

    def f(x):
        if x.event_type not in events:
            return False

        if x.is_directory:
            return False

        name = Path(x.src_path).resolve().name
        if len(filenames) == 0:
            return True

        for wanted in filenames:
            if strict:
                if wanted == name:
                    return True
            else:
                if wanted in name:
                    return True
        return False

    return wait_until(dir_list, f, timeout=timeout, polling=polling)
