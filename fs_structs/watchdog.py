"""Wait for filesystem events with the ``watchdog`` package.

The observer is chosen per path. On Linux, inotify does not report changes made by other
hosts on NFS/CIFS mounts, so paths on a network mount get a ``PollingObserver``. Windows
(ReadDirectoryChangesW, also over SMB) and macOS keep the native observer. Override with
the module variable ``FORCE_POLLING`` (True/False) or the environment variable
``FS_STRUCTS_POLLING`` ("1" / "0").

Notifications can be lost on network filesystems: callers always pass a timeout and
re-check the state after it.
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

# Filesystem types (as listed in /proc/mounts) that inotify cannot watch for remote changes.
NETWORK_FSTYPES = {"nfs", "nfs4", "cifs", "smb3", "smbfs", "fuse.sshfs", "9p", "afs", "glusterfs", "ceph"}

FORCE_POLLING = None  # None = decide per path; True/False = force
POLLING_INTERVAL = 1.0  # seconds, PollingObserver


def _mount_fstype(path, mounts_file="/proc/mounts"):
    """fstype of the longest mount point that contains ``path`` (POSIX only), or None."""
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
        mount = parts[1].replace("\\040", " ")  # /proc/mounts escapes spaces
        fstype = parts[2]
        # Later lines shadow earlier ones with the same mount point (rootfs, then the real root).
        if (path == mount or path.startswith(mount.rstrip("/") + "/")) and len(mount) >= len(best_mount):
            best_mount, best_type = mount, fstype
    return best_type


def _needs_polling(paths, mounts_file="/proc/mounts"):
    """True when any of ``paths`` needs a PollingObserver (see module docstring)."""
    if FORCE_POLLING is not None:
        return bool(FORCE_POLLING)
    env = os.environ.get("FS_STRUCTS_POLLING")
    if env is not None:
        return env.strip().lower() not in ("", "0", "false", "no")
    if not sys.platform.startswith("linux"):
        return False
    return any(_mount_fstype(p, mounts_file) in NETWORK_FSTYPES for p in paths)


def _observer_for(paths, polling=None):
    use_polling = _needs_polling(paths) if polling is None else polling
    return PollingObserver(timeout=POLLING_INTERVAL) if use_polling else Observer()


class FSEventHandler(FileSystemEventHandler):
    """Sets a ``threading.Event`` on the first event accepted by the filter ``f``.

    Attributes:
        f (callable): takes an event and returns a boolean.
        event_obj: the accepted event, or None.
    """

    def __init__(self, f=lambda x: True):
        self.f = f
        self.event_obj = None
        self.event = threading.Event()

    def on_any_event(self, event):
        if self.f(event):
            self.event_obj = event
            self.event.set()


def wait_until(dir_list, f=lambda x: True, timeout=60, polling=None):
    """Wait until an event accepted by ``f`` happens in any directory of ``dir_list``.

    Args:
        dir_list (list): directories to monitor (not recursive).
        f (callable): filter applied to events; default accepts everything.
        timeout (float): maximum seconds to wait.
        polling (bool | None): force (True) or forbid (False) the PollingObserver;
            None chooses per path.

    Returns:
        tuple: ``(event, event_type, parent_dir, name)`` of the accepted event, or
        ``(None, None, None, None)`` on timeout.
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
    """Wait for a file (not directory) event in ``dir_list``.

    Args:
        filenames (list): accept only these names (``strict``: equality; otherwise
            substring). Empty: any file.
        events (list): event types to accept; empty: all of ``event_types``.
    Returns: as ``wait_until``.
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
