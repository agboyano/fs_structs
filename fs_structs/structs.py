"""Filesystem data structures for persistent data and for distributed processes.

Filesystem data structures that make persistent data management easy, let distributed
processes that only share a directory or a filesystem share data and state, and communicate
with each other. The library works on Windows and Linux machines.

The module has three structures and one lock:

- ``FSUDict``: an unordered dict. Every key is a file in a directory.
- ``FSList``: a list stored in a directory. It can be used as a FIFO queue shared by
  several processes: producers ``append``, consumers ``pop_left``.
- ``FSNamespace``: a directory that holds named dicts, lists and sub-namespaces.
- ``lock_context`` (with ``acquire_lock`` and ``release_lock``): a lock that lets only one
  process at a time, on any machine, run a piece of code.

What the module guarantees:

- Writes are atomic. A reader sees the old value or the new value, never a half-written file.
- Locks are exclusive on local disks and on SMB and NFS shares.
- ``FSList.pop_left`` gives every element to exactly one consumer.

Requirements and known limits are listed in README.md.

Examples:
    >>> import tempfile
    >>> from fs_structs.structs import FSNamespace
    >>> ns = FSNamespace(tempfile.mkdtemp())
    >>> prices = ns.udict("prices")
    >>> prices["AAPL"] = 187.5
    >>> prices["AAPL"]
    187.5
    >>> queue = ns.list("requests")
    >>> queue.append({"id": 1})
    >>> queue.pop_left()
    {'id': 1}
"""

import errno
import functools
import json
import logging
import os
import pickle
import random
import shutil
import socket
import time
import uuid
from ast import literal_eval
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import joblib

from .watchdog import wait_until

logger = logging.getLogger(__name__)

# Longest file name accepted by NTFS, ext4 and the SMB/NFS servers in common use.
MAX_FILENAME_LEN = 255
# Longest full path on Windows unless long paths are enabled in the registry.
MAX_WINDOWS_PATH_LEN = 259

# Windows error codes that surface as OSError on network shares and are worth retrying:
# 59 unexpected network error, 64 network name deleted, 121 semaphore timeout.
_TRANSIENT_WINERRORS = {59, 64, 121}

_SKIP = object()  # sentinel: "this file is not one of ours"


def timestamp():
    """Return the current local time as an ISO 8601 string.

    Returns:
        str: For example ``'2026-09-16T10:31:07.123456'``.

    Examples:
        >>> from fs_structs.structs import timestamp
        >>> ts = timestamp()
        >>> ts[4] == "-" and ts[10] == "T"
        True
    """
    return datetime.now().isoformat()


def sleep(a, b=None):
    """Sleep for a fixed time, or for a random time inside an interval.

    Args:
        a (float): Seconds to sleep. If ``b`` is given, the lower limit of the interval.
        b (float, optional): Upper limit of the interval. The real time is a uniform random
            value between ``a`` and ``b``.

    Examples:
        >>> from fs_structs.structs import sleep
        >>> sleep(0.01)
        >>> sleep(0.01, 0.02)
    """
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a, b))


def _is_transient(exc):
    """Return True for errors that usually go away if the operation is repeated.

    Args:
        exc (Exception): The error raised by a file operation.

    Returns:
        bool: True for ``PermissionError`` (Windows: the file is open in another process)
        and for the network error codes in ``_TRANSIENT_WINERRORS``.
    """
    if isinstance(exc, PermissionError):
        return True
    return isinstance(exc, OSError) and getattr(exc, "winerror", None) in _TRANSIENT_WINERRORS


def _is_cross_device(exc):
    """Return True when a rename failed because source and target are on different volumes.

    Args:
        exc (Exception): The error raised by ``os.replace``.

    Returns:
        bool: True for ``EXDEV`` on POSIX and for Windows error 17.
    """
    return isinstance(exc, OSError) and (
        exc.errno == errno.EXDEV or getattr(exc, "winerror", None) == 17
    )


@functools.lru_cache(maxsize=None)
def _windows_long_paths_enabled():
    """Return False only on Windows with ``LongPathsEnabled = 0`` (paths limited to 259 chars).

    Returns:
        bool: True on every other system, and on Windows when long paths are enabled.
    """
    if os.name != "nt":
        return True
    try:
        import winreg

        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\FileSystem")
        return winreg.QueryValueEx(key, "LongPathsEnabled")[0] == 1
    except OSError:
        return False


def _retry(fn, tries=5, wait=(0.05, 0.25)):
    """Call ``fn()`` and repeat it when it fails with a transient error.

    Args:
        fn (callable): Function without arguments.
        tries (int): Maximum number of calls.
        wait (tuple): ``(min, max)`` random seconds to sleep between calls.

    Returns:
        The value returned by ``fn()``.

    Raises:
        OSError: The last error, when every call failed. Errors that are not transient
            (for example ``FileNotFoundError``) are raised at once, without repeating.
    """
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except OSError as e:
            if not _is_transient(e) or attempt == tries:
                raise
            logger.debug("transient error, retry %d/%d: %s", attempt, tries, e)
            sleep(*wait)


# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------


@dataclass
class FSSerializer:
    """How values are written to and read from files.

    Three serializers are ready to use: ``joblib_serializer`` (default, good for NumPy and
    pandas objects), ``pickle_serializer`` and ``json_serializer``. You can build your own
    for any other format.

    Args:
        dump (callable): ``dump(value, filename)`` writes ``value`` to the file.
        load (callable): ``load(filename)`` reads the file and returns the value.
        extension (str): File extension without the dot, for example ``"json"``.

    See Also:
        FSUDict: Receives a serializer in its ``serializer`` argument.

    Examples:
        A serializer that stores plain text:

        >>> import tempfile
        >>> from fs_structs.structs import FSSerializer, FSUDict
        >>> def dump_text(value, filename):
        ...     with open(filename, "w", encoding="utf-8") as f:
        ...         f.write(value)
        >>> def load_text(filename):
        ...     with open(filename, "r", encoding="utf-8") as f:
        ...         return f.read()
        >>> text_serializer = FSSerializer(dump_text, load_text, "txt")
        >>> root = tempfile.mkdtemp()
        >>> notes = FSUDict(root + "/notes", root + "/tmp", serializer=text_serializer)
        >>> notes["today"] = "call the bank"
        >>> notes["today"]
        'call the bank'
    """

    dump: callable
    load: callable
    extension: str


# Implementation notes:
# - The file is flushed and fsync'ed so the data reaches the disk (or the network server)
#   before the atomic rename done by FSUDict.
def pickle_dump(value, filename):
    """Write ``value`` to ``filename`` with ``pickle`` (highest protocol).

    Args:
        value: Any object that ``pickle`` can serialize.
        filename (str | Path): File to create or overwrite.

    See Also:
        pickle_load: Reads the file back.
        pickle_serializer: The ``FSSerializer`` that uses this pair of functions.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import pickle_dump, pickle_load
        >>> path = tempfile.mkdtemp() + "/value.pkl"
        >>> pickle_dump({"a": (1, 2)}, path)
        >>> pickle_load(path)
        {'a': (1, 2)}
    """
    with open(filename, "wb") as f:
        pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)
        f.flush()
        os.fsync(f.fileno())


def pickle_load(filename):
    """Read a value written by ``pickle_dump``.

    Warning: ``pickle`` runs code while loading. Only read files from directories that you
    trust.

    Args:
        filename (str | Path): File to read.

    Returns:
        The stored object.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import pickle_dump, pickle_load
        >>> path = tempfile.mkdtemp() + "/value.pkl"
        >>> pickle_dump([1, 2, 3], path)
        >>> pickle_load(path)
        [1, 2, 3]
    """
    with open(filename, "rb") as f:
        return pickle.load(f)


pickle_serializer = FSSerializer(pickle_dump, pickle_load, "pkl")
"""FSSerializer: values stored with ``pickle``; extension ``pkl``."""


def json_dump(value, filename):
    """Write ``value`` to ``filename`` as UTF-8 JSON.

    JSON keeps only str, int, float, bool, None, lists and dicts with string keys. Tuples
    come back as lists and dict keys come back as strings.

    Args:
        value: A JSON-compatible object.
        filename (str | Path): File to create or overwrite.

    See Also:
        json_load: Reads the file back.
        json_serializer: The ``FSSerializer`` that uses this pair of functions.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import json_dump, json_load
        >>> path = tempfile.mkdtemp() + "/value.json"
        >>> json_dump({"city": "Málaga", "temp": (30, 31)}, path)
        >>> json_load(path)
        {'city': 'Málaga', 'temp': [30, 31]}
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())


def json_load(filename):
    """Read a value written by ``json_dump`` (UTF-8 on every platform).

    Args:
        filename (str | Path): File to read.

    Returns:
        The stored object.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import json_dump, json_load
        >>> path = tempfile.mkdtemp() + "/value.json"
        >>> json_dump([1, "two"], path)
        >>> json_load(path)
        [1, 'two']
    """
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


json_serializer = FSSerializer(json_dump, json_load, "json")
"""FSSerializer: values stored as JSON text; extension ``json``."""


def joblib_dump(value, filename):
    """Write ``value`` to ``filename`` with ``joblib`` (efficient for NumPy and pandas).

    Args:
        value: Any object that ``joblib`` can serialize.
        filename (str | Path): File to create or overwrite.

    See Also:
        joblib_load: Reads the file back.
        joblib_serializer: The default ``FSSerializer``.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import joblib_dump, joblib_load
        >>> path = tempfile.mkdtemp() + "/value.jbl"
        >>> joblib_dump({"weights": [0.5, 0.5]}, path)
        >>> joblib_load(path)
        {'weights': [0.5, 0.5]}
    """
    with open(filename, "wb") as f:
        joblib.dump(value, f)  # joblib.dump accepts a path or a file handle
        f.flush()
        os.fsync(f.fileno())


def joblib_load(filename):
    """Read a value written by ``joblib_dump``.

    Warning: like ``pickle``, ``joblib`` runs code while loading. Only read files from
    directories that you trust.

    Args:
        filename (str | Path): File to read.

    Returns:
        The stored object.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import joblib_dump, joblib_load
        >>> path = tempfile.mkdtemp() + "/value.jbl"
        >>> joblib_dump(3.25, path)
        >>> joblib_load(path)
        3.25
    """
    return joblib.load(filename)


joblib_serializer = FSSerializer(joblib_dump, joblib_load, "jbl")
"""FSSerializer: values stored with ``joblib``; extension ``jbl``. This is the default."""


# ---------------------------------------------------------------------------
# FSUDict
# ---------------------------------------------------------------------------


# Implementation notes:
# - File name = hex(repr(key)) + "." + extension. Hex keeps every character legal on NTFS,
#   ext4, SMB and NFS, and works the same on case-insensitive and case-sensitive systems.
#   The key is recovered with ast.literal_eval, so keys must be Python literals.
# - Write (fast=False): dump to temp_dir/tmp_<uuid>, then Path.replace onto the final
#   name. os.replace is atomic on the same volume; across volumes it fails (EXDEV /
#   WinError 17) and we raise a clear ValueError.
# - Network shares: on an SMB/Samba or NFS share the rename is executed by the server,
#   so it stays atomic for every client as long as temp_dir is on the same share (the
#   default base_path/tmp is). Verified on an SMB share from Windows: the whole suite,
#   including the multi-process queue tests, passes with FS_STRUCTS_TEST_ROOT on the share.
#   SMB clients raise transient OSErrors under load (WinError 59, 64, 121): _retry covers
#   them.
# - pop (fast=False): the file is first renamed into temp_dir, then loaded. Only one of
#   several concurrent callers can win the rename, so a key is handed to one caller only.
# - Every file operation goes through _retry: PermissionError (Windows, file open in
#   another process) and transient network errors are repeated a few times.
# - keys() ignores directories (locks live inside the data directory), files with another
#   extension and names that do not decode (Thumbs.db, desktop.ini, orphan temp files).
# - _key_to_filename raises ValueError when the name exceeds 255 characters or, on Windows
#   without LongPathsEnabled, when the full path exceeds 259 characters.
# - clear() is shutil.rmtree + mkdir and is NOT concurrency-safe, by design (administrative
#   operation): rmtree is file by file, so a failure leaves a partial state; a concurrent
#   writer's rename into the removed directory raises FileNotFoundError (not retried); lock
#   directories inside are removed; on Windows a process that has a file open or a
#   ReadDirectoryChangesW watch on the directory (a blocked pop_left) can make the rmtree
#   or the mkdir fail. Readers only see KeyError. Per-file atomicity is never violated.
class FSUDict:
    """Unordered dict stored as one file per key in a directory.

    Use it to keep data between runs of a program, or to share values between processes
    that can see the same directory, on one machine or on several machines.

    It is **unordered**: ``keys()``, ``values()``, ``items()`` and iteration return the
    elements in the order of the directory listing. That order is arbitrary and can change
    from one filesystem to another. Sort the result when you need a fixed order.

    Writes are atomic. A reader always gets the old value or the new value, never a partial
    file. For this the value is first written to ``temp_dir`` and then renamed, so
    ``temp_dir`` must be on the same volume as ``base_path``.

    Args:
        base_path (str | Path): Directory where the values are stored. Created if missing.
        temp_dir (str | Path): Directory for temporary files. Same volume as ``base_path``.
        serializer (FSSerializer): How values are written and read. Default: joblib.
        fast (bool): If True, values are written directly, without atomic operations.
            Faster, but not safe for distributed processes: a reader may see a partial
            file. Default False.
        clean (bool): If True, the dict is emptied (``clear()``) before it is returned.
            Default False. ``clean=True`` is not atomic (see ``clear()``).

    Keys must be Python literals: str, int, float, bytes, None, and tuples, lists, dicts or
    sets made of them. ``repr(key)`` should stay under about 125 characters.

    Raises:
        ValueError: If a key is too long for the filesystem, or if ``temp_dir`` is on
            another volume than ``base_path``.
        KeyError: When reading, deleting or popping a key that does not exist.

    See Also:
        FSList: A list (and FIFO queue) built on top of this class.
        FSNamespace: Creates dicts and lists by name under one directory.
        FSSerializer: To store values in another format.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import FSUDict
        >>> root = tempfile.mkdtemp()
        >>> prices = FSUDict(root + "/prices", root + "/tmp")
        >>> prices["AAPL"] = 187.5
        >>> prices[("AAPL", "2026-01-02")] = 188.0
        >>> prices["AAPL"]
        187.5
        >>> len(prices)
        2
        >>> "AAPL" in prices
        True
        >>> sorted(prices.keys(), key=repr)     # unordered: sort before printing
        ['AAPL', ('AAPL', '2026-01-02')]
        >>> del prices["AAPL"]
        >>> prices.get("AAPL", "not found")
        'not found'

        A second object on the same directory sees the same data, also from another
        process or another machine:

        >>> same = FSUDict(root + "/prices", root + "/tmp")
        >>> same[("AAPL", "2026-01-02")]
        188.0

        ``clean=True`` empties the dict before returning it, for a fresh start:

        >>> fresh = FSUDict(root + "/prices", root + "/tmp", clean=True)
        >>> len(fresh)
        0
    """

    def __init__(self, base_path, temp_dir, serializer=joblib_serializer, fast=False, clean=False):
        self.base_path = Path(base_path).resolve()
        self.temp_dir = Path(temp_dir).resolve()

        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.load = serializer.load
        self.dump = serializer.dump
        self.ext = serializer.extension

        self.fast = fast

        if clean:
            self.clear()

    # -- key <-> file name ---------------------------------------------------

    def _key_to_filename(self, key):
        """Return the file name for ``key``.

        Args:
            key: A Python literal.

        Returns:
            str: ``hex(repr(key)) + "." + extension``.

        Raises:
            ValueError: If the name, or the full path on Windows, is too long.
        """
        name = repr(key).encode().hex() + "." + self.ext
        if len(name) > MAX_FILENAME_LEN:
            raise ValueError(
                f"key too long: repr(key) has {len(repr(key))} characters and the file name "
                f"would have {len(name)} (limit {MAX_FILENAME_LEN})"
            )
        full = len(str(self.base_path)) + 1 + len(name)
        if full > MAX_WINDOWS_PATH_LEN and not _windows_long_paths_enabled():
            raise ValueError(
                f"key too long for this Windows: the full path would have {full} characters "
                f"(limit {MAX_WINDOWS_PATH_LEN} unless LongPathsEnabled is set in the registry); "
                f"use a shorter key or a shorter base_path"
            )
        return name

    def _filename_to_key(self, filename):
        """Return the key stored under ``filename``.

        Args:
            filename (str): Name of one of our files.

        Returns:
            The key.

        Raises:
            ValueError, SyntaxError: If the name is not hex or not a Python literal.
        """
        s = filename.rsplit(".", 1)[0]
        return literal_eval(bytes.fromhex(s).decode())

    def _try_filename_to_key(self, filename):
        """Return the key for one of our files, or ``_SKIP`` for any other file.

        Args:
            filename (str): Name of a file found in ``base_path``.

        Returns:
            The key, or the ``_SKIP`` sentinel.
        """
        if not filename.endswith("." + self.ext):
            return _SKIP
        try:
            return self._filename_to_key(filename)
        except (ValueError, SyntaxError) as e:
            logger.debug("ignoring foreign file %s in %s: %s", filename, self.base_path, e)
            return _SKIP

    def _temp_path(self):
        """Return a new unique path in ``temp_dir``."""
        return self.temp_dir / f"tmp_{uuid.uuid4().hex}"

    # -- atomic rename -----------------------------------------------------------

    def _replace(self, src, dst):
        """Rename ``src`` onto ``dst`` with retries.

        Args:
            src (Path): Existing file.
            dst (Path): Final name; overwritten if it exists.

        Raises:
            ValueError: If ``src`` and ``dst`` are on different volumes.
            OSError: Any other error, after the retries.
        """
        try:
            try:
                _retry(lambda: src.replace(dst))
            except PermissionError:
                # Windows: the target is still open elsewhere after the retries.
                # Not atomic, but the best available: remove and rename.
                dst.unlink(missing_ok=True)
                src.replace(dst)
        except OSError as e:
            if _is_cross_device(e):
                raise ValueError(
                    f"temp_dir ({src.parent}) must be on the same volume as base_path "
                    f"({dst.parent}): an atomic rename across volumes is impossible"
                ) from e
            raise

    # -- mapping protocol -----------------------------------------------------------

    def __setitem__(self, key, value):
        """Store ``value`` under ``key`` (``d[key] = value``). Atomic unless ``fast``.

        See Also:
            FSUDict: Examples on the class.
        """
        target_path = self.base_path / self._key_to_filename(key)

        if self.fast:
            _retry(lambda: self.dump(value, target_path))
            return

        temp_path = self._temp_path()
        try:
            self.dump(value, temp_path)
            self._replace(temp_path, target_path)
        finally:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError as e:
                    logger.debug("could not remove temp file %s: %s", temp_path, e)

    def __getitem__(self, key):
        """Return the value of ``key`` (``d[key]``). Raises ``KeyError`` if it is missing.

        See Also:
            FSUDict: Examples on the class.
        """
        target_path = self.base_path / self._key_to_filename(key)
        try:
            return _retry(lambda: self.load(target_path))
        except FileNotFoundError:
            raise KeyError(key) from None

    def __delitem__(self, key):
        """Remove ``key`` (``del d[key]``). Raises ``KeyError`` if it is missing.

        See Also:
            FSUDict: Examples on the class.
        """
        target_path = self.base_path / self._key_to_filename(key)
        try:
            _retry(target_path.unlink)
        except FileNotFoundError:
            raise KeyError(key) from None

    def __contains__(self, key):
        """Return True if ``key`` exists (``key in d``).

        See Also:
            FSUDict: Examples on the class.
        """
        return (self.base_path / self._key_to_filename(key)).is_file()

    def keys(self):
        """Return the list of keys, in arbitrary order.

        Files that do not belong to the dict (other extensions, undecodable names,
        directories) are ignored.

        Returns:
            list: The keys.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSUDict
            >>> root = tempfile.mkdtemp()
            >>> d = FSUDict(root + "/d", root + "/tmp")
            >>> d["b"] = 2
            >>> d["a"] = 1
            >>> sorted(d.keys())
            ['a', 'b']
        """
        entries = _retry(lambda: list(os.scandir(self.base_path)))
        keys = []
        for entry in entries:
            if not entry.is_file():
                continue
            key = self._try_filename_to_key(entry.name)
            if key is not _SKIP:
                keys.append(key)
        return keys

    def values(self):
        """Return the list of values, in the same arbitrary order as ``keys()``.

        Returns:
            list: The values.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSUDict
            >>> root = tempfile.mkdtemp()
            >>> d = FSUDict(root + "/d", root + "/tmp")
            >>> d.update({"a": 1, "b": 2})
            >>> sorted(d.values())
            [1, 2]
        """
        return [self[key] for key in self.keys()]

    def items(self):
        """Return the list of ``(key, value)`` pairs, in arbitrary order.

        Returns:
            list: The pairs.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSUDict
            >>> root = tempfile.mkdtemp()
            >>> d = FSUDict(root + "/d", root + "/tmp")
            >>> d.update({"a": 1, "b": 2})
            >>> sorted(d.items())
            [('a', 1), ('b', 2)]
        """
        return [(key, self[key]) for key in self.keys()]

    def __len__(self):
        """Return the number of keys (``len(d)``).

        See Also:
            FSUDict: Examples on the class.
        """
        return len(self.keys())

    def __iter__(self):
        """Iterate over the keys in arbitrary order (``for key in d``).

        See Also:
            FSUDict: Examples on the class.
        """
        return (k for k in self.keys())

    def clear(self):
        """Remove every key.

        The whole directory is deleted and created again, so lock directories inside it
        are deleted too.

        Not safe with other processes: call it only when no other process is using the
        dict, for example when a job starts (see ``clean=True``). It is not atomic, and a
        process writing at the same time can fail with ``FileNotFoundError``. The data of
        other keys is never corrupted.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSUDict
            >>> root = tempfile.mkdtemp()
            >>> d = FSUDict(root + "/d", root + "/tmp")
            >>> d["a"] = 1
            >>> d.clear()
            >>> len(d)
            0
        """
        _retry(lambda: shutil.rmtree(self.base_path))
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)  # temp_dir may live inside base_path

    def update(self, iterable):
        """Store several keys at once.

        Args:
            iterable: A mapping, or an iterable of ``(key, value)`` pairs.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSUDict
            >>> root = tempfile.mkdtemp()
            >>> d = FSUDict(root + "/d", root + "/tmp")
            >>> d.update({"a": 1})
            >>> d.update([("b", 2), ("c", 3)])
            >>> sorted(d.items())
            [('a', 1), ('b', 2), ('c', 3)]
        """
        if isinstance(iterable, Mapping):
            for k in iterable:
                self[k] = iterable[k]
        else:
            for k, v in iterable:
                self[k] = v

    def get(self, key, default=None):
        """Return the value of ``key``, or ``default`` if the key does not exist.

        Args:
            key: The key.
            default: Value returned when the key is missing. Default None.

        Returns:
            The value or ``default``.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSUDict
            >>> root = tempfile.mkdtemp()
            >>> d = FSUDict(root + "/d", root + "/tmp")
            >>> d["a"] = 1
            >>> d.get("a")
            1
            >>> d.get("z", 0)
            0
        """
        if key in self:
            return self[key]
        return default

    def pop(self, key):
        """Remove ``key`` and return its value.

        When several processes call ``pop`` on the same key at the same time, only one of
        them gets the value; the others get ``KeyError``. This is what makes ``FSList``
        a safe queue.

        Args:
            key: The key to remove.

        Returns:
            The value that was stored.

        Raises:
            KeyError: If the key does not exist (or another process took it first).

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSUDict
            >>> root = tempfile.mkdtemp()
            >>> d = FSUDict(root + "/d", root + "/tmp")
            >>> d["job"] = {"id": 7}
            >>> d.pop("job")
            {'id': 7}
            >>> "job" in d
            False
        """
        target_path = self.base_path / self._key_to_filename(key)
        if self.fast:
            value = self[key]
            del self[key]
            return value

        temp_path = self._temp_path()
        try:
            self._replace(target_path, temp_path)
        except FileNotFoundError:
            raise KeyError(key) from None
        try:
            return _retry(lambda: self.load(temp_path))
        finally:
            try:
                temp_path.unlink()
            except OSError as e:
                logger.debug("could not remove temp file %s: %s", temp_path, e)


# ---------------------------------------------------------------------------
# FSList
# ---------------------------------------------------------------------------


# Implementation notes:
# - Keys are tuples of ints that sort in insertion order: (time_ns, random). Rear keys use
#   the current time; front keys its negative, so insert(0, ...) sorts before everything.
# - A key between two existing keys bisects the first component that leaves a gap of more
#   than 2; when no component does, a new component is appended (tuples compare element by
#   element, so a longer tuple sorts between its neighbours).
# - time_ns() is time.perf_counter_ns anchored to time.time() at construction, because
#   time.time() has about 1 µs resolution and time.time_ns() is not reliable on Windows.
#   Equal consecutive values are bumped by 1 ns inside one process.
class TimeOrderedTuple:
    """Generator of keys that sort in the order they were created. Used by ``FSList``.

    Every key is a tuple of integers. You do not need to understand the values: the only
    property that matters is that a key created later sorts after a key created earlier
    (``new_rear_tuple``), a "front" key sorts before every other key
    (``new_front_tuple``), and a "mid" key sorts between two given keys (``new_mid_tuple``).

    Args:
        max_int (int): Upper limit used when a new tuple component is appended.
        max_random (int): Upper limit of the random component that breaks ties.

    See Also:
        FSList: Uses one generator per list object.

    Examples:
        >>> from fs_structs.structs import TimeOrderedTuple
        >>> gen = TimeOrderedTuple()
        >>> first = gen.new_rear_tuple()
        >>> second = gen.new_rear_tuple()
        >>> first < second
        True
        >>> gen.new_front_tuple() < first
        True
        >>> first < gen.new_mid_tuple(first, second) < second
        True
    """

    def __init__(self, max_int=1000_000_000, max_random=1000_000):
        self.max_int = max_int
        self.max_random = max_random
        self.last_rear_time = 0
        self.last_front_time = 0
        self.epoch_start = time.time()
        self.perf_start = time.perf_counter_ns()

    def time_ns(self):
        """Return the current time in nanoseconds since the epoch, with high resolution.

        Returns:
            int: Nanoseconds.

        Examples:
            >>> from fs_structs.structs import TimeOrderedTuple
            >>> gen = TimeOrderedTuple()
            >>> gen.time_ns() <= gen.time_ns()
            True
        """
        elapsed_ns = time.perf_counter_ns() - self.perf_start
        return int(self.epoch_start * 1e9) + elapsed_ns

    def random_int(self):
        """Return a random integer in ``[0, max_random)``.

        Returns:
            int: The random value.

        Examples:
            >>> from fs_structs.structs import TimeOrderedTuple
            >>> gen = TimeOrderedTuple(max_random=10)
            >>> 0 <= gen.random_int() < 10
            True
        """
        return int(random.random() * self.max_random)

    def new_rear_tuple(self):
        """Return a key that sorts after every key created before it by this generator.

        Returns:
            tuple: ``(time_ns, random_int)``.

        Examples:
            >>> from fs_structs.structs import TimeOrderedTuple
            >>> gen = TimeOrderedTuple()
            >>> gen.new_rear_tuple() < gen.new_rear_tuple()
            True
        """
        elapsed = self.time_ns()
        if elapsed == self.last_rear_time:
            elapsed += 1
        self.last_rear_time = elapsed
        return (elapsed, self.random_int())

    def new_front_tuple(self):
        """Return a key that sorts before every rear key and before earlier front keys.

        Returns:
            tuple: ``(-time_ns, random_int)``.

        Examples:
            >>> from fs_structs.structs import TimeOrderedTuple
            >>> gen = TimeOrderedTuple()
            >>> rear = gen.new_rear_tuple()
            >>> front1 = gen.new_front_tuple()
            >>> front2 = gen.new_front_tuple()
            >>> front2 < front1 < rear
            True
        """
        elapsed = -self.time_ns()
        if elapsed == self.last_front_time:
            elapsed -= 1
        self.last_front_time = elapsed
        return (elapsed, self.random_int())

    def new_mid_tuple(self, prev_tuple, next_tuple):
        """Return a key that sorts between ``prev_tuple`` and ``next_tuple``.

        Args:
            prev_tuple (tuple): The smaller key.
            next_tuple (tuple): The larger key.

        Returns:
            tuple: A key with ``prev_tuple < key < next_tuple``. It may have more
            components than its neighbours.

        Examples:
            >>> from fs_structs.structs import TimeOrderedTuple
            >>> gen = TimeOrderedTuple()
            >>> a, b = gen.new_rear_tuple(), gen.new_rear_tuple()
            >>> mid = gen.new_mid_tuple(a, b)
            >>> a < mid < b
            True
            >>> a < gen.new_mid_tuple(a, mid) < mid
            True
        """
        N_prev = len(prev_tuple)
        N_next = len(next_tuple)
        N = max(N_prev, N_next)

        new_tuple = []
        for i in range(N - 1):
            prev_item = prev_tuple[i] if i <= (N_prev - 1) else 0
            next_item = next_tuple[i] if i <= (N_next - 1) else self.max_int

            new_tuple.append(prev_item)
            diff_int = next_item - prev_item
            if diff_int > 2:
                new_tuple[i] += int(diff_int / 2.0)
                new_tuple.append(self.random_int())
                return tuple(new_tuple)

        new_tuple.append(self.max_int)
        new_tuple.append(self.random_int())
        return tuple(new_tuple)


# Implementation notes:
# - Storage is an FSUDict whose keys come from TimeOrderedTuple; keys() sorts them, so
#   every indexed access costs a directory listing plus a sort (O(n log n)).
# - pop_left (fast=False) takes the directory lock "pop_left_lock" inside the list
#   directory, so consumers on different hosts never take the same element. Producers do
#   not lock: the atomic rename of FSUDict.__setitem__ is enough.
# - Order between hosts depends on their clocks: FIFO is exact inside one process and
#   approximate across machines.
# - Differences from list: items() returns (value, key) pairs; slice assignment with more
#   values than slots appends the surplus at the end instead of inserting it; insert()
#   does not accept negative indexes.
class FSList:
    """List stored in a directory. It can be used as a FIFO queue shared by several processes.

    As a **list**: ``append``, ``insert``, indexing, slicing, ``pop``, ``del``, ``len`` and
    iteration work like in a Python list, but every access reads the directory, so it is
    made for queues and small lists, not for big random-access sequences.

    As a **FIFO queue**: producers call ``append`` and consumers call ``pop_left``.
    ``pop_left`` uses a lock, so each element is given to exactly one consumer even when
    the consumers run on different machines. Elements appended by different machines are
    ordered by each machine's clock, so the order between machines is approximate.

    Args:
        base_path (str | Path): Directory where the elements are stored. Created if missing.
        temp_dir (str | Path): Directory for temporary files. Same volume as ``base_path``.
        serializer (FSSerializer): How elements are written and read. Default: joblib.
        fast (bool): If True, elements are written without atomic operations and
            ``pop_left`` takes no lock. Faster, but not safe for distributed processes.
            Default False.
        clean (bool): If True, the list is emptied (``clear()``) before it is returned.
            Default False. ``clean=True`` is not atomic (see ``clear()``).

    See Also:
        FSUDict: The storage under the list.
        FSNamespace: Creates lists by name under one directory.
        TimeOrderedTuple: Generates the keys that keep the order.

    Examples:
        As a list:

        >>> import tempfile
        >>> from fs_structs.structs import FSList
        >>> root = tempfile.mkdtemp()
        >>> lst = FSList(root + "/lst", root + "/tmp")
        >>> lst.extend(["b", "c"])
        >>> lst.insert(0, "a")
        >>> lst.values()
        ['a', 'b', 'c']
        >>> lst[1]
        'b'
        >>> lst[-1] = "z"
        >>> list(lst)
        ['a', 'b', 'z']
        >>> len(lst)
        3

        As a FIFO queue (a consumer would usually be another process):

        >>> queue = FSList(root + "/queue", root + "/tmp")
        >>> queue.append({"job": 1})
        >>> queue.append({"job": 2})
        >>> queue.pop_left()
        {'job': 1}
        >>> queue.pop_left()
        {'job': 2}
        >>> len(queue)
        0

        ``clean=True`` empties the list before returning it:

        >>> queue.append("left over")
        >>> fresh = FSList(root + "/queue", root + "/tmp", clean=True)
        >>> len(fresh)
        0
    """

    def __init__(self, base_path, temp_dir, serializer=joblib_serializer, fast=False, clean=False):
        self.data = FSUDict(base_path, temp_dir, serializer, fast=fast, clean=clean)
        self.base_path = self.data.base_path
        self.serializer = serializer
        self.key_generator = TimeOrderedTuple()
        self.id = uuid.uuid4().hex  # v7 is time ordered, not implemented yet
        self.fast = fast

    def _append_key(self):
        """Return a new key that sorts after every existing key."""
        return self.key_generator.new_rear_tuple()

    def _new_zero_key(self):
        """Return a new key that sorts before every existing key."""
        return self.key_generator.new_front_tuple()

    def _new_mid_key(self, prev_key, next_key):
        """Return a new key that sorts between ``prev_key`` and ``next_key``."""
        return self.key_generator.new_mid_tuple(prev_key, next_key)

    def append(self, value):
        """Add ``value`` at the end of the list.

        Args:
            value: The element to add.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> lst = FSList(root + "/lst", root + "/tmp")
            >>> lst.append(10)
            >>> lst.append(20)
            >>> lst.values()
            [10, 20]
        """
        new_key = self._append_key()
        assert new_key not in self.data
        self.data[new_key] = value

    def extend(self, iterable):
        """Add every element of ``iterable`` at the end, in order.

        Args:
            iterable: The elements to add.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> lst = FSList(root + "/lst", root + "/tmp")
            >>> lst.extend(range(3))
            >>> lst.values()
            [0, 1, 2]
        """
        for x in iterable:
            self.append(x)

    def copy(self):
        """Return the elements as a plain Python list (not a new ``FSList``).

        Returns:
            list: The elements in order.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> lst = FSList(root + "/lst", root + "/tmp")
            >>> lst.extend([1, 2])
            >>> lst.copy()
            [1, 2]
        """
        return [x for x in self]

    def insert(self, index, value):
        """Insert ``value`` before position ``index``.

        Args:
            index (int): Position, from 0. A value equal to or larger than ``len(lst)``
                appends at the end. Negative indexes are not accepted.
            value: The element to insert.

        Raises:
            IndexError: If ``index`` is negative.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> lst = FSList(root + "/lst", root + "/tmp")
            >>> lst.extend(["b", "d"])
            >>> lst.insert(0, "a")
            >>> lst.insert(2, "c")
            >>> lst.insert(99, "e")
            >>> lst.values()
            ['a', 'b', 'c', 'd', 'e']
        """
        keys = self.keys()
        N = len(keys)

        if index == 0:
            if N == 0:
                self.append(value)
            else:
                self.data[self._new_zero_key()] = value
        elif index >= N:
            self.append(value)
        elif 0 < index < N:
            self.data[self._new_mid_key(keys[index - 1], keys[index])] = value
        else:
            raise IndexError("list assignment index out of range")

    def values(self):
        """Return the elements in order, as a Python list.

        Returns:
            list: The elements.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> lst = FSList(root + "/lst", root + "/tmp")
            >>> lst.extend(["x", "y"])
            >>> lst.values()
            ['x', 'y']
        """
        return [self.data[k] for k in self.keys()]

    def items(self):
        """Return ``(value, key)`` pairs in order. Note the order: value first, then key.

        Returns:
            list: The pairs. Keys are the internal tuples that keep the order.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> lst = FSList(root + "/lst", root + "/tmp")
            >>> lst.extend(["x", "y"])
            >>> [value for value, key in lst.items()]
            ['x', 'y']
        """
        return [(self.data[k], k) for k in self.keys()]

    def keys(self):
        """Return the internal keys, sorted. They define the order of the elements.

        Returns:
            list: Tuples of integers, from first element to last.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> lst = FSList(root + "/lst", root + "/tmp")
            >>> lst.extend(["x", "y"])
            >>> keys = lst.keys()
            >>> len(keys) == 2 and keys[0] < keys[1]
            True
        """
        return sorted(self.data.keys())

    def __delitem__(self, index):
        """Remove the element at ``index`` (``del lst[index]``).

        See Also:
            FSList: Examples on the class.
        """
        del self.data[self.keys()[index]]

    def clear(self):
        """Remove every element.

        Not safe with other processes: stop producers and consumers first. Clearing a
        queue while consumers run has no defined result; a consumer blocked in
        ``pop_left`` may also fail, on Windows, when its directory is removed under it.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> lst = FSList(root + "/lst", root + "/tmp")
            >>> lst.extend([1, 2, 3])
            >>> lst.clear()
            >>> len(lst)
            0
        """
        self.data.clear()

    def __getitem__(self, index):
        """Return one element (``lst[i]``) or a list of elements (``lst[a:b]``).

        See Also:
            FSList: Examples on the class.
        """
        if isinstance(index, slice):
            return [self.data[k] for k in self.keys()[index]]
        return self.data[self.keys()[index]]

    def __setitem__(self, index, value):
        """Replace one element (``lst[i] = v``) or a slice (``lst[a:b] = values``).

        With a slice and more values than positions, the surplus values are appended at
        the end (a Python list would insert them).

        See Also:
            FSList: Examples on the class.
        """
        if isinstance(index, slice):
            ks = self.keys()
            li = [x for x in range(len(ks))[index]]
            step = 1 if index.step is None else index.step
            if (len(li) != len(value)) and step != 1:
                raise ValueError(
                    f"attempt to assign sequence of size {len(value)} to extended slice of size {len(li)}"
                )

            i = 0
            for j in li:
                self.data[ks[j]] = value[i]
                i += 1

            if i < (len(value)):
                for j in range(i, len(value)):
                    self.append(value[j])
        else:
            self.data[self.keys()[index]] = value

    def __del__(self):
        pass

    def __contains__(self, key):
        """Return True if some element equals ``key`` (``value in lst``). Reads every element.

        See Also:
            FSList: Examples on the class.
        """
        return key in [x for x in self]

    def __iter__(self):
        """Iterate over the elements in order (``for x in lst``).

        See Also:
            FSList: Examples on the class.
        """
        return (self.data[k] for k in self.keys())

    def __len__(self):
        """Return the number of elements (``len(lst)``).

        See Also:
            FSList: Examples on the class.
        """
        return len(self.keys())

    def pop(self, index=-1):
        """Remove and return the element at ``index`` (the last one by default).

        This method does not take a lock. Use it when only one process removes elements
        from the list; with several consumers use ``pop_left``.

        Args:
            index (int): Position; negative values count from the end. Default -1.

        Returns:
            The removed element.

        Raises:
            IndexError: If the list is empty or ``index`` is out of range.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> lst = FSList(root + "/lst", root + "/tmp")
            >>> lst.extend([1, 2, 3])
            >>> lst.pop()
            3
            >>> lst.pop(0)
            1
            >>> lst.values()
            [2]
        """
        ix = self.keys()[index]
        return self.data.pop(ix)

    def pop_left(self, timeout=-1.0, watchdog_timeout=19, wait=(0.0, 0.0), max_age=None):
        """Remove and return the first element. Safe with several consumers.

        Consumers on any machine can call ``pop_left`` on the same list: a lock inside the
        list directory makes sure that each element is returned to one consumer only.

        Args:
            timeout (float): Seconds to wait for the lock. Negative: wait without limit.
                0: try once. Default -1.
            watchdog_timeout (float): Seconds to wait for the filesystem event of the lock
                release before checking again. Default 19.
            wait (tuple): ``(min, max)`` random seconds to sleep after the event, to spread
                competing consumers. Default ``(0.0, 0.0)``.
            max_age (float, optional): If given, a lock older than this many seconds is
                treated as left by a dead process and is broken. Default None: never.

        Returns:
            The first element.

        Raises:
            IndexError: If the list is empty.
            LockingError: If the lock could not be taken within ``timeout``.

        See Also:
            acquire_lock: Meaning of ``timeout``, ``watchdog_timeout``, ``wait`` and ``max_age``.
            pop: Removal without a lock, for a single consumer.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSList
            >>> root = tempfile.mkdtemp()
            >>> queue = FSList(root + "/queue", root + "/tmp")
            >>> queue.extend(["first", "second"])
            >>> queue.pop_left(timeout=10)
            'first'
            >>> queue.pop_left(timeout=10)
            'second'
            >>> try:
            ...     queue.pop_left(timeout=10)
            ... except IndexError:
            ...     print("the queue is empty")
            the queue is empty
        """

        def first():
            try:
                ix = self.keys()[0]
            except IndexError:
                raise IndexError("pop_left: empty list") from None
            return self.data.pop(ix)

        if self.fast:
            return first()
        with lock_context(self.data.base_path, "pop_left_lock", timeout, watchdog_timeout, wait, max_age):
            return first()
        # Unreachable in the original implementation, kept for reference:
        # raise IndexError("pop_left: empty list")  # It should not happen


# ---------------------------------------------------------------------------
# FSNamespace
# ---------------------------------------------------------------------------


# Implementation notes:
# - A variable is a sub-directory named "<prefix>_<name>": ud_ for dicts, li_ for lists,
#   ns_ for sub-namespaces. names_types() parses those directory names; other directories
#   (for example "tmp" or "*.lock") are ignored.
# - All variables of a namespace, and of its sub-namespaces, share one temp_dir
#   (base_path/tmp by default) so that renames stay on the same volume.
# - __getattr__ only runs when normal attribute lookup fails. Names starting with "_" and
#   lookups on a half-built instance (copy, pickle) raise AttributeError without touching
#   the disk; unknown variables also raise AttributeError so hasattr() works.
# - clean=True calls clear() on the object being returned, nothing more: udict/list clear
#   that one variable; namespace(name, clean=True) clears the sub-namespace "name" (all its
#   variables and sub-namespaces), never the parent; FSNamespace(path, clean=True) clears
#   the whole namespace at "path". The default clear() also removes tmp_* files in temp_dir.
# - Duplicated names: udict()/list()/namespace() refuse to create a name that exists with
#   another type, but the check and the mkdir are two steps, so two processes racing, a
#   directory made by hand or a direct FSUDict/FSList on the namespace path can leave both
#   ud_name and li_name. variable() and attribute access then raise ValueError instead of
#   returning whichever directory the listing gives first (the list on NTFS, either on
#   ext4). type(), udict(), list() and namespace() keep using the first entry found.
#   hasattr(ns, name) propagates that ValueError (it only swallows AttributeError).
class FSNamespace:
    """A directory that holds named dicts, lists and sub-namespaces.

    A namespace gives a name to every shared structure, so that different programs can
    open the same dict or queue just by knowing the directory and the name. Variables can
    be reached as attributes: ``ns.prices`` is the same as ``ns.variable("prices")``.

    Args:
        base_path (str | Path): Root directory of the namespace. Created if missing.
        temp_dir (str | Path, optional): Directory for temporary files. Default
            ``base_path/tmp``. Must be on the same volume as ``base_path``.
        serializer (FSSerializer): Serializer for every variable. Default: joblib.
        clean (bool): If True, every variable and sub-namespace is removed (``clear()``)
            before the namespace is returned. Default False. ``clean=True`` is not atomic
            (see ``clear()``).

    See Also:
        FSUDict: The dict returned by ``udict``.
        FSList: The list returned by ``list``.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import FSNamespace
        >>> ns = FSNamespace(tempfile.mkdtemp())
        >>> ns.udict("settings")["threads"] = 4
        >>> ns.list("jobs").append("job-1")
        >>> sorted(ns.names_types())
        [('jobs', 'li'), ('settings', 'ud')]
        >>> ns.settings["threads"]
        4
        >>> ns.jobs.pop_left()
        'job-1'
        >>> sub = ns.namespace("reports")
        >>> sub.udict("daily")["rows"] = 120
        >>> ns.reports.daily["rows"]
        120

        Namespaces and structures can be chained in one expression. A nested namespace and
        a dict inside it are created on first use:

        >>> ns.namespace("config").udict("user")["name"] = "Federico"
        >>> ns.config.user["name"]
        'Federico'

        ``clean=True`` empties a structure before returning it. Here the dict is emptied,
        then the new value is stored, so only ``"name"`` remains:

        >>> ns.config.user["role"] = "admin"
        >>> ns.namespace("config").udict("user", clean=True)["name"] = "Ana"
        >>> sorted(ns.config.user.items())
        [('name', 'Ana')]

        A fresh queue inside a fresh sub-namespace, in one line:

        >>> ns.namespace("batch", clean=True).list("queue").append("job-1")
        >>> ns.batch.queue.values()
        ['job-1']

        ``clean=True`` on the namespace itself removes everything under it:

        >>> FSNamespace(ns.base_path, clean=True).names()
        []
    """

    def __init__(self, base_path, temp_dir=None, serializer=joblib_serializer, clean=False):
        self.base_path = Path(base_path).resolve()
        if temp_dir is None:
            self.temp_dir = self.base_path / "tmp"
        else:
            self.temp_dir = Path(temp_dir).resolve()

        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.serializer = serializer
        self.sep = "_"
        self.udict_prefix = "ud"
        self.list_prefix = "li"
        self.namespace_prefix = "ns"
        self.prefixes = {self.udict_prefix, self.list_prefix, self.namespace_prefix}

        if clean:
            self.clear()

    def _check_type(self, name, prefix):
        """Raise ``ValueError`` if ``name`` exists with a type other than ``prefix``.

        Args:
            name (str): Variable name.
            prefix (str): Expected type prefix (``ud``, ``li`` or ``ns``).
        """
        nt = [x for x in self.names_types() if x[0] == name]
        if len(nt) > 0 and nt[0][1] != prefix:
            raise ValueError(f"{name} exists with type {nt[0][1]}")

    def udict(self, name, fast=False, clean=False):
        """Return the dict called ``name``, creating it if it does not exist.

        Args:
            name (str): Variable name.
            fast (bool): Passed to ``FSUDict``. Default False.
            clean (bool): If True, the dict is emptied (``clear()``) before it is returned.
                Default False. ``clean=True`` is not atomic (see ``clear()``).

        Returns:
            FSUDict: The dict.

        Raises:
            ValueError: If ``name`` already exists as a list or a namespace.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSNamespace
            >>> ns = FSNamespace(tempfile.mkdtemp())
            >>> d = ns.udict("settings")
            >>> d["mode"] = "test"
            >>> ns.udict("settings")["mode"]
            'test'
            >>> ns.udict("settings", clean=True).get("mode", "empty")
            'empty'
        """
        self._check_type(name, self.udict_prefix)
        return FSUDict(
            self.base_path / (self.udict_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
            fast=fast,
            clean=clean,
        )

    def list(self, name, fast=False, clean=False):
        """Return the list called ``name``, creating it if it does not exist.

        Args:
            name (str): Variable name.
            fast (bool): Passed to ``FSList``. Default False.
            clean (bool): If True, the list is emptied (``clear()``) before it is returned.
                Default False. ``clean=True`` is not atomic (see ``clear()``).

        Returns:
            FSList: The list.

        Raises:
            ValueError: If ``name`` already exists as a dict or a namespace.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSNamespace
            >>> ns = FSNamespace(tempfile.mkdtemp())
            >>> ns.list("queue").append("a")
            >>> ns.list("queue").values()
            ['a']
            >>> len(ns.list("queue", clean=True))
            0
        """
        self._check_type(name, self.list_prefix)
        return FSList(
            self.base_path / (self.list_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
            fast=fast,
            clean=clean,
        )

    def namespace(self, name, clean=False):
        """Return the sub-namespace called ``name``, creating it if it does not exist.

        Args:
            name (str): Variable name.
            clean (bool): If True, everything under the sub-namespace is removed
                (``clear()``) before it is returned. The parent is not touched. Default False.
                ``clean=True`` is not atomic (see ``clear()``).

        Returns:
            FSNamespace: The sub-namespace. It shares ``temp_dir`` and the serializer.

        Raises:
            ValueError: If ``name`` already exists as a dict or a list.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSNamespace
            >>> ns = FSNamespace(tempfile.mkdtemp())
            >>> ns.namespace("team").udict("members")["n"] = 3
            >>> ns.namespace("team").udict("members")["n"]
            3
            >>> ns.type("team")
            'ns'
            >>> ns.namespace("team", clean=True).names()
            []
            >>> ns.type("team")        # the parent still has the (now empty) sub-namespace
            'ns'
        """
        self._check_type(name, self.namespace_prefix)
        return FSNamespace(
            self.base_path / (self.namespace_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
            clean=clean,
        )

    def clear(self, clear_tmp=True):
        """Remove every variable and sub-namespace.

        Not safe with other processes: call it only when no other process is using the
        namespace. With ``clear_tmp=True`` (default) it also removes temporary files that
        another process may be writing at that moment, and that process fails.

        Args:
            clear_tmp (bool): If True, also remove the library's ``tmp_*`` files from
                ``temp_dir``. Be careful when several namespaces share the same
                ``temp_dir``: a file being written by another one could be removed.
                Default True.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSNamespace
            >>> ns = FSNamespace(tempfile.mkdtemp())
            >>> ns.udict("a")["k"] = 1
            >>> ns.clear()
            >>> ns.names()
            []
        """
        for d in [d for d in self.base_path.iterdir() if d.is_dir() if d != self.temp_dir]:
            _retry(lambda d=d: shutil.rmtree(d))
        for f in [f for f in self.base_path.iterdir() if f.is_file()]:
            _retry(f.unlink)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        if clear_tmp:
            self.clear_tmp()

    def clear_tmp(self):
        """Remove the library's ``tmp_*`` files from ``temp_dir``. Other files are kept.

        Temporary files are normally removed at once; they stay only when a process dies
        in the middle of a write.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSNamespace
            >>> ns = FSNamespace(tempfile.mkdtemp())
            >>> _ = (ns.temp_dir / "tmp_orphan").write_bytes(b"x")
            >>> ns.clear_tmp()
            >>> (ns.temp_dir / "tmp_orphan").exists()
            False
        """
        for f in [f for f in self.temp_dir.iterdir() if f.is_file() and f.name.startswith("tmp_")]:
            try:
                f.unlink()
            except OSError as e:
                logger.debug("could not remove %s: %s", f, e)

    def _dirname_to_name_type(self, dirname):
        """Split a directory name ``"<prefix>_<name>"`` into ``(name, prefix)``."""
        s = (dirname).split(self.sep)
        return (self.sep).join(s[1:]), s[0]

    def names_types(self):
        """Return ``(name, type)`` pairs for every variable, in arbitrary order.

        Returns:
            list: Pairs where type is ``'ud'`` (dict), ``'li'`` (list) or ``'ns'``
            (sub-namespace).

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSNamespace
            >>> ns = FSNamespace(tempfile.mkdtemp())
            >>> _ = ns.udict("a"), ns.list("b"), ns.namespace("c")
            >>> sorted(ns.names_types())
            [('a', 'ud'), ('b', 'li'), ('c', 'ns')]
        """
        tmp = [
            self._dirname_to_name_type(d.name)
            for d in self.base_path.iterdir()
            if d.is_dir()
        ]
        return [x for x in tmp if x[1] in self.prefixes]

    def names(self):
        """Return the variable names, in arbitrary order.

        Returns:
            list: The names.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSNamespace
            >>> ns = FSNamespace(tempfile.mkdtemp())
            >>> _ = ns.udict("a"), ns.list("b")
            >>> sorted(ns.names())
            ['a', 'b']
        """
        return [x[0] for x in self.names_types()]

    def type(self, name):
        """Return the type of variable ``name``: ``'ud'``, ``'li'``, ``'ns'`` or None.

        Args:
            name (str): Variable name.

        Returns:
            str | None: The type, or None if the variable does not exist.

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSNamespace
            >>> ns = FSNamespace(tempfile.mkdtemp())
            >>> _ = ns.list("jobs")
            >>> ns.type("jobs")
            'li'
            >>> ns.type("missing") is None
            True
        """
        try:
            return [x[1] for x in self.names_types() if x[0] == name][0]
        except IndexError:
            return None

    def _types_of(self, name):
        """Return the sorted list of types (``ud``, ``li``, ``ns``) that exist for ``name``.

        Args:
            name (str): Variable name.

        Returns:
            list: Empty if the name does not exist; more than one entry if the name exists
            as directories of several types.
        """
        return sorted(t for n, t in self.names_types() if n == name)

    def variable(self, name, fast=False, clean=False):
        """Return the existing variable ``name``, whatever its type.

        Args:
            name (str): Variable name.
            fast (bool): Passed to ``FSUDict`` or ``FSList``. Ignored for namespaces.
            clean (bool): If True, the variable is emptied (``clear()``) before it is
                returned. Default False. ``clean=True`` is not atomic (see ``clear()``).

        Returns:
            FSUDict | FSList | FSNamespace: The variable.

        Raises:
            ValueError: If no variable has that name (use ``udict``, ``list`` or
                ``namespace`` to create one), or if the name exists with more than one
                type (two directories, for example ``ud_name`` and ``li_name``).

        Examples:
            >>> import tempfile
            >>> from fs_structs.structs import FSNamespace
            >>> ns = FSNamespace(tempfile.mkdtemp())
            >>> ns.udict("a")["k"] = 1
            >>> ns.variable("a")["k"]
            1
            >>> ns.a["k"]        # same thing, as an attribute
            1

            A name that exists with two types is an error, not a silent choice:

            >>> ns.udict("dup")["k"] = 1
            >>> (ns.base_path / "li_dup").mkdir()     # same name as a list, made by hand
            >>> try:
            ...     ns.variable("dup")
            ... except ValueError as e:
            ...     print(e)
            dup exists with several types in ...: li, ud; remove one of the directories
        """
        types = self._types_of(name)
        if len(types) > 1:
            raise ValueError(
                f"{name} exists with several types in {self.base_path}: {', '.join(types)}; "
                "remove one of the directories"
            )
        if len(types) == 0:
            raise ValueError(f"{name} not available in namespace")

        if types[0] == self.udict_prefix:
            return FSUDict(
                self.base_path / (self.udict_prefix + self.sep + name),
                self.temp_dir,
                self.serializer,
                fast,
                clean,
            )
        if types[0] == self.list_prefix:
            return FSList(
                self.base_path / (self.list_prefix + self.sep + name),
                self.temp_dir,
                self.serializer,
                fast,
                clean,
            )
        return FSNamespace(
            self.base_path / (self.namespace_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
            clean=clean,
        )

    def __getattr__(self, name):
        """Return ``self.variable(name)`` for attribute access (``ns.prices``).

        Raises:
            AttributeError: If the variable does not exist, or for private names.
            ValueError: If the name exists with more than one type (see ``variable``).

        See Also:
            variable: The method behind this attribute access.
        """
        if name.startswith("_") or "base_path" not in self.__dict__:
            raise AttributeError(name)
        if len(self._types_of(name)) > 1:
            return self.variable(name)  # raises the "several types" ValueError
        try:
            return self.variable(name)
        except ValueError as e:
            raise AttributeError(str(e)) from None


# ---------------------------------------------------------------------------
# Locks
# ---------------------------------------------------------------------------


class LockingError(Exception):
    """Raised by ``acquire_lock`` when the lock was not taken within ``timeout`` seconds.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import LockingError, acquire_lock, release_lock
        >>> root = tempfile.mkdtemp()
        >>> acquire_lock(root, "job")
        True
        >>> try:
        ...     acquire_lock(root, "job", timeout=0)
        ... except LockingError:
        ...     print("someone else has the lock")
        someone else has the lock
        >>> release_lock(root, "job")
    """

    def __init__(self, message):
        super().__init__(message)


def _lock_dir(base_path, lock_name):
    """Return the lock directory ``base_path/<lock_name>.lock``."""
    return Path(base_path) / (lock_name + ".lock")


def _write_owner(lock_dir):
    """Write ``lock_dir/owner`` with host, pid and time. Informative and used for expiry.

    Args:
        lock_dir (Path): The lock directory, already created.
    """
    info = {"host": socket.gethostname(), "pid": os.getpid(), "time": time.time(), "iso": timestamp()}
    try:
        with open(lock_dir / "owner", "w", encoding="utf-8") as f:
            json.dump(info, f)
    except OSError as e:
        logger.debug("could not write lock owner in %s: %s", lock_dir, e)


def _read_owner(lock_dir):
    """Return the content of ``lock_dir/owner`` as a dict, or None if unreadable."""
    try:
        with open(lock_dir / "owner", "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _lock_age(lock_dir):
    """Return the seconds since the lock was taken, or None if the lock is gone.

    The mtime of the ``owner`` file is used; for a lock without it (older versions), the
    mtime of the directory.
    """
    for p in (lock_dir / "owner", lock_dir):
        try:
            return time.time() - p.stat().st_mtime
        except OSError:
            continue
    return None


def _break_stale_lock(lock_dir, max_age):
    """Remove ``lock_dir`` if it is older than ``max_age`` seconds.

    The directory is first renamed to ``<name>.stale-<uuid>``. The rename is atomic, so
    when several waiters try at the same time only one of them breaks the lock.

    Args:
        lock_dir (Path): The lock directory.
        max_age (float): Age limit in seconds.

    Returns:
        bool: True if the lock was broken by this call.
    """
    age = _lock_age(lock_dir)
    if age is None or age <= max_age:
        return False
    stale = lock_dir.with_name(f"{lock_dir.name}.stale-{uuid.uuid4().hex}")
    try:
        lock_dir.rename(stale)
    except OSError:
        return False  # released or broken by someone else meanwhile
    logger.warning(
        "breaking stale lock %s: age %.0fs > max_age %ss, owner %s",
        lock_dir, age, max_age, _read_owner(stale),
    )
    try:
        _retry(lambda: shutil.rmtree(stale))
    except OSError as e:
        logger.warning("could not remove broken lock %s: %s", stale, e)
    return True


# Implementation notes:
# - The lock is a directory: os.mkdir either creates it or fails with FileExistsError,
#   atomically, on local disks and on SMB and NFS shares. No file content is needed.
# - SMB/Samba: the mkdir is executed by the server, so two clients on different machines
#   never both succeed. Verified on an SMB share from Windows: 4 processes x 50 locked
#   increments give exactly 200, and a waiter wakes up ~0.5 s after the release because
#   Windows receives the deletion notification from the server. Linux clients on a CIFS
#   mount do not receive remote notifications: fs_structs.watchdog switches to polling
#   there (see that module), so the wake-up takes up to POLLING_INTERVAL seconds.
# - max_age compares the mtime of the owner file, assigned by the server on SMB, with the
#   local clock: keep the machine clocks in sync and use minutes, not seconds.
# - After mkdir an "owner" file (host, pid, time) is written inside. The lock is valid
#   from the mkdir; the file is for diagnosis and for max_age.
# - Waiters block on a watchdog event: a "deleted" or "moved" entry with the lock name.
#   Windows reports a removed directory as a file deletion, so is_directory is not
#   checked. A lost event only costs the watchdog_timeout: the state is re-checked after it.
# - max_age: _break_stale_lock renames the old directory (atomic, one winner) and removes
#   it, then the caller tries mkdir again.
def acquire_lock(base_path, lock_name, timeout=-1.0, watchdog_timeout=19, wait=(0.0, 0.0), max_age=None):
    """Take the lock ``lock_name`` in directory ``base_path``. Only one holder at a time.

    Processes on any machine that see ``base_path`` compete for the same lock. Prefer
    ``lock_context`` (a ``with`` block) so the lock is always released.

    Args:
        base_path (str | Path): Directory where the lock lives (``<lock_name>.lock``).
        lock_name (str): Name of the lock.
        timeout (float): Seconds to wait if the lock is taken. Negative: wait without
            limit. 0: try once. Default -1.
        watchdog_timeout (float): Seconds to wait for the filesystem event of the release
            before checking the lock again. Default 19.
        wait (tuple): ``(min, max)`` random seconds to sleep after the event, so that
            several waiters do not all try at the same instant. Default ``(0.0, 0.0)``.
        max_age (float, optional): If given, a lock older than this many seconds is
            treated as left behind by a dead process: it is broken and taken. Use minutes,
            not seconds, and keep the machine clocks in sync. Default None: never break.

    Returns:
        bool: True when the lock is taken.

    Raises:
        LockingError: If the lock was not taken within ``timeout``.

    See Also:
        release_lock: Frees the lock.
        lock_context: Takes and frees the lock around a ``with`` block.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import acquire_lock, release_lock
        >>> root = tempfile.mkdtemp()
        >>> acquire_lock(root, "nightly-report")
        True
        >>> release_lock(root, "nightly-report")

        A lock left by a process that died an hour ago is broken when ``max_age`` allows it:

        >>> import os, time
        >>> acquire_lock(root, "old")
        True
        >>> hour_ago = time.time() - 3600
        >>> os.utime(os.path.join(root, "old.lock", "owner"), (hour_ago, hour_ago))
        >>> acquire_lock(root, "old", timeout=0, max_age=600)
        True
        >>> release_lock(root, "old")
    """
    dir_name = lock_name + ".lock"
    lock_dir = Path(base_path) / dir_name

    def try_lock():
        try:
            os.mkdir(lock_dir)
        except (FileExistsError, PermissionError):
            return False
        _write_owner(lock_dir)
        return True

    def attempt():
        if try_lock():
            return True
        if max_age is not None and _break_stale_lock(lock_dir, max_age):
            return try_lock()
        return False

    def released(event):
        return event.event_type in ("deleted", "moved") and Path(event.src_path).name == dir_name

    if attempt():
        logger.debug("%s set in %s", lock_name, base_path)
        return True

    start_time = time.time()
    time_left = timeout
    wait_forever = timeout < -0.001

    while time_left > 0.0 or wait_forever:
        new_watchdog_timeout = watchdog_timeout if wait_forever else min(watchdog_timeout, time_left)
        wait_until([base_path], released, timeout=new_watchdog_timeout)
        sleep(*wait)

        if attempt():
            logger.debug("%s set in %s", lock_name, base_path)
            return True

        time_left = timeout - (time.time() - start_time)

    raise LockingError(f"{timestamp()} Couldn't get lock {lock_name} in {base_path}")


def release_lock(base_path, lock_name):
    """Free the lock ``lock_name`` in ``base_path``.

    If the lock is already gone (another process broke it as stale), a warning is logged
    and nothing is raised.

    Args:
        base_path (str | Path): Directory where the lock lives.
        lock_name (str): Name of the lock.

    See Also:
        acquire_lock: Takes the lock.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import acquire_lock, release_lock
        >>> root = tempfile.mkdtemp()
        >>> acquire_lock(root, "job")
        True
        >>> release_lock(root, "job")
        >>> acquire_lock(root, "job", timeout=0)    # free again
        True
        >>> release_lock(root, "job")
    """
    lock_dir = _lock_dir(base_path, lock_name)
    try:
        _retry(lambda: shutil.rmtree(lock_dir))
    except FileNotFoundError:
        logger.warning("lock %s in %s was already gone (broken as stale or released twice)", lock_name, base_path)
        return
    logger.debug("%s unset in %s", lock_name, base_path)


@contextmanager
def lock_context(base_path, lock_name, timeout=60, watchdog_timeout=19, wait=(0.0, 0.0), max_age=None):
    """Run a ``with`` block while holding the lock. The lock is freed even on error.

    Args:
        base_path (str | Path): Directory where the lock lives.
        lock_name (str): Name of the lock.
        timeout (float): Seconds to wait for the lock. Default 60.
        watchdog_timeout (float): See ``acquire_lock``. Default 19.
        wait (tuple): See ``acquire_lock``. Default ``(0.0, 0.0)``.
        max_age (float, optional): See ``acquire_lock``. Default None.

    Raises:
        LockingError: If the lock was not taken within ``timeout``.

    See Also:
        acquire_lock: Meaning of every argument.

    Examples:
        >>> import tempfile
        >>> from fs_structs.structs import FSUDict, lock_context
        >>> root = tempfile.mkdtemp()
        >>> counter = FSUDict(root + "/counter", root + "/tmp")
        >>> with lock_context(counter.base_path, "increment", timeout=30):
        ...     counter["n"] = counter.get("n", 0) + 1     # read-modify-write, one process at a time
        >>> counter["n"]
        1
    """
    acquire_lock(base_path, lock_name, timeout, watchdog_timeout, wait, max_age)
    try:
        yield
    finally:
        release_lock(base_path, lock_name)
