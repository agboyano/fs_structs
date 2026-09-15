"""Data structures stored on a directory, shared or local.

- ``FSUDict``: a dict where every key is a file. Writes are atomic (temp file + rename).
- ``FSList``: a list built on ``FSUDict`` with time-ordered tuple keys; ``pop_left`` is safe
  with several consumers.
- ``FSNamespace``: a directory of named dicts, lists and sub-namespaces.
- ``acquire_lock`` / ``release_lock`` / ``lock_context``: directory locks (``os.mkdir`` is
  atomic on local disks, SMB and NFS) with optional expiry of stale locks.

Guarantees, requirements and known limitations are documented in README.md.
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
    return datetime.now().isoformat()


def sleep(a, b=None):
    """Sleep `a` seconds, or a uniform random time in [a, b] when `b` is given."""
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a, b))


def _is_transient(exc):
    """PermissionError (Windows: file open elsewhere) or a transient network OSError."""
    if isinstance(exc, PermissionError):
        return True
    return isinstance(exc, OSError) and getattr(exc, "winerror", None) in _TRANSIENT_WINERRORS


def _is_cross_device(exc):
    """rename() across volumes: EXDEV on POSIX, WinError 17 on Windows."""
    return isinstance(exc, OSError) and (
        exc.errno == errno.EXDEV or getattr(exc, "winerror", None) == 17
    )


@functools.lru_cache(maxsize=None)
def _windows_long_paths_enabled():
    """True unless this is Windows with LongPathsEnabled = 0 (paths limited to 259 chars)."""
    if os.name != "nt":
        return True
    try:
        import winreg

        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\FileSystem")
        return winreg.QueryValueEx(key, "LongPathsEnabled")[0] == 1
    except OSError:
        return False


def _retry(fn, tries=5, wait=(0.05, 0.25)):
    """Call ``fn()``; on a transient OSError sleep a random ``wait`` and retry.

    Up to ``tries`` attempts; the last error is re-raised. FileNotFoundError and other
    non-transient errors are raised immediately.
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
    dump: callable
    load: callable
    extension: str


def pickle_dump(value, filename):
    with open(filename, "wb") as f:
        pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)
        f.flush()
        os.fsync(f.fileno())


def pickle_load(filename):
    with open(filename, "rb") as f:
        return pickle.load(f)


pickle_serializer = FSSerializer(pickle_dump, pickle_load, "pkl")


def json_dump(value, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())


def json_load(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


json_serializer = FSSerializer(json_dump, json_load, "json")


def joblib_dump(value, filename):
    with open(filename, "wb") as f:
        joblib.dump(value, f)  # joblib.dump accepts a path or a file handle
        f.flush()
        os.fsync(f.fileno())


def joblib_load(filename):
    return joblib.load(filename)


joblib_serializer = FSSerializer(joblib_dump, joblib_load, "jbl")


# ---------------------------------------------------------------------------
# FSUDict
# ---------------------------------------------------------------------------


class FSUDict:
    """Dict stored as one file per key under ``base_path``.

    The file name is ``hex(repr(key)) + "." + extension``, so keys must round-trip through
    ``repr`` / ``ast.literal_eval`` (str, int, float, bytes, None, tuples, lists, dicts and
    sets of those). Files with another extension or an undecodable name are ignored.

    With ``fast=False`` (default) a value is written to ``temp_dir`` and then renamed onto its
    final name, which is atomic: readers see the old value or the new one, never a partial
    file. ``temp_dir`` must be on the same volume as ``base_path``. With ``fast=True`` the value
    is written in place (not atomic).

    ``clear()`` removes the whole directory, including any lock directories inside it.
    """

    def __init__(self, base_path, temp_dir, serializer=joblib_serializer, fast=False):
        self.base_path = Path(base_path).resolve()
        self.temp_dir = Path(temp_dir).resolve()

        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.load = serializer.load
        self.dump = serializer.dump
        self.ext = serializer.extension

        self.fast = fast

    # -- key <-> file name ---------------------------------------------------

    def _key_to_filename(self, key):
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
        s = filename.rsplit(".", 1)[0]
        return literal_eval(bytes.fromhex(s).decode())

    def _try_filename_to_key(self, filename):
        """Key for one of our files, or ``_SKIP`` for any other file in the directory."""
        if not filename.endswith("." + self.ext):
            return _SKIP
        try:
            return self._filename_to_key(filename)
        except (ValueError, SyntaxError) as e:
            logger.debug("ignoring foreign file %s in %s: %s", filename, self.base_path, e)
            return _SKIP

    def _temp_path(self):
        return self.temp_dir / f"tmp_{uuid.uuid4().hex}"

    # -- atomic rename -----------------------------------------------------------

    def _replace(self, src, dst):
        """Rename ``src`` onto ``dst`` with retries; clear error for cross-volume moves."""
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
        """Store ``value``; atomic (temp file + rename) unless ``fast``."""
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
        target_path = self.base_path / self._key_to_filename(key)
        try:
            return _retry(lambda: self.load(target_path))
        except FileNotFoundError:
            raise KeyError(key) from None

    def __delitem__(self, key):
        target_path = self.base_path / self._key_to_filename(key)
        try:
            _retry(target_path.unlink)
        except FileNotFoundError:
            raise KeyError(key) from None

    def __contains__(self, key):
        return (self.base_path / self._key_to_filename(key)).is_file()

    def keys(self):
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
        return [self[key] for key in self.keys()]

    def items(self):
        return [(key, self[key]) for key in self.keys()]

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return (k for k in self.keys())

    def clear(self):
        """Remove every key (the whole directory, lock directories included)."""
        _retry(lambda: shutil.rmtree(self.base_path))
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)  # temp_dir may live inside base_path

    def update(self, iterable):
        if isinstance(iterable, Mapping):
            for k in iterable:
                self[k] = iterable[k]
        else:
            for k, v in iterable:
                self[k] = v

    def get(self, key, default=None):
        if key in self:
            return self[key]
        return default

    def pop(self, key):
        """Remove ``key`` and return its value.

        Unless ``fast``, the file is first renamed into ``temp_dir`` so that only one of
        several concurrent callers can get it.
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


class TimeOrderedTuple:
    """Generates sortable tuple keys ``(time_ns, random_int)`` for ``FSList``.

    Rear keys use the current time, front keys its negative, so appends sort after and
    ``insert(0, ...)`` before everything else. A key between two existing keys bisects the
    first component that leaves room, appending a component when none does. Time comes
    from ``time.perf_counter_ns`` anchored to ``time.time`` at construction (``time.time``
    has ~1 µs resolution and ``time.time_ns`` is unreliable on Windows). Order across
    processes and hosts therefore depends on their clocks: FIFO is approximate there.
    """

    def __init__(self, max_int=1000_000_000, max_random=1000_000):
        self.max_int = max_int
        self.max_random = max_random
        self.last_rear_time = 0
        self.last_front_time = 0
        self.epoch_start = time.time()
        self.perf_start = time.perf_counter_ns()

    def time_ns(self):
        elapsed_ns = time.perf_counter_ns() - self.perf_start
        return int(self.epoch_start * 1e9) + elapsed_ns

    def random_int(self):
        return int(random.random() * self.max_random)

    def new_rear_tuple(self):
        elapsed = self.time_ns()
        if elapsed == self.last_rear_time:
            elapsed += 1
        self.last_rear_time = elapsed
        return (elapsed, self.random_int())

    def new_front_tuple(self):
        elapsed = -self.time_ns()
        if elapsed == self.last_front_time:
            elapsed -= 1
        self.last_front_time = elapsed
        return (elapsed, self.random_int())

    def new_mid_tuple(self, prev_tuple, next_tuple):
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


class FSList:
    """List stored as an ``FSUDict`` whose keys are time-ordered tuples.

    Every indexed access lists and sorts the directory, so it suits queues and small
    lists rather than large random-access sequences. ``items()`` returns ``(value, key)``
    pairs. Slice assignment appends the surplus values at the end instead of inserting
    them (unlike ``list``).
    """

    def __init__(self, base_path, temp_dir, serializer=joblib_serializer, fast=False):
        self.data = FSUDict(base_path, temp_dir, serializer, fast=fast)
        self.base_path = self.data.base_path
        self.serializer = serializer
        self.key_generator = TimeOrderedTuple()
        self.id = uuid.uuid4().hex  # v7 is time ordered, not implemented yet
        self.fast = fast

    def _append_key(self):
        return self.key_generator.new_rear_tuple()

    def _new_zero_key(self):
        return self.key_generator.new_front_tuple()

    def _new_mid_key(self, prev_key, next_key):
        return self.key_generator.new_mid_tuple(prev_key, next_key)

    def append(self, value):
        new_key = self._append_key()
        assert new_key not in self.data
        self.data[new_key] = value

    def extend(self, iterable):
        for x in iterable:
            self.append(x)

    def copy(self):
        """Return the contents as a plain Python list."""
        return [x for x in self]

    def insert(self, index, value):
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
        return [self.data[k] for k in self.keys()]

    def items(self):
        return [(self.data[k], k) for k in self.keys()]

    def keys(self):
        return sorted(self.data.keys())

    def __delitem__(self, index):
        del self.data[self.keys()[index]]

    def clear(self):
        self.data.clear()

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self.data[k] for k in self.keys()[index]]
        return self.data[self.keys()[index]]

    def __setitem__(self, index, value):
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
        return key in [x for x in self]

    def __iter__(self):
        return (self.data[k] for k in self.keys())

    def __len__(self):
        return len(self.keys())

    def pop(self, index=-1):
        ix = self.keys()[index]
        return self.data.pop(ix)

    def pop_left(self, timeout=-1.0, watchdog_timeout=19, wait=(0.0, 0.0), max_age=None):
        """Remove and return the first element.

        Unless ``fast``, the extraction is protected by the directory lock ``pop_left_lock``
        inside the list directory, so several consumers on different hosts never take the
        same element. Writers (``append``) do not take the lock: the atomic rename makes
        that unnecessary.

        timeout: seconds to wait for the lock (< 0 = forever, 0 = try once).
        watchdog_timeout, wait, max_age: as in ``acquire_lock``.
        Raises IndexError if the list is empty and LockingError if the lock is not acquired
        in time.
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


class FSNamespace:
    """Directory of named dicts (``ud_<name>``), lists (``li_<name>``) and sub-namespaces
    (``ns_<name>``), all sharing one ``temp_dir`` (``base_path/tmp`` by default).

    Variables can be reached as attributes: ``ns.prices`` is ``ns.variable("prices")``.
    """

    def __init__(self, base_path, temp_dir=None, serializer=joblib_serializer):
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

    def _check_type(self, name, prefix):
        nt = [x for x in self.names_types() if x[0] == name]
        if len(nt) > 0 and nt[0][1] != prefix:
            raise ValueError(f"{name} exists with type {nt[0][1]}")

    def udict(self, name, fast=False):
        self._check_type(name, self.udict_prefix)
        return FSUDict(
            self.base_path / (self.udict_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
            fast=fast,
        )

    def list(self, name, fast=False):
        self._check_type(name, self.list_prefix)
        return FSList(
            self.base_path / (self.list_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
            fast=fast,
        )

    def namespace(self, name):
        self._check_type(name, self.namespace_prefix)
        return FSNamespace(
            self.base_path / (self.namespace_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
        )

    def clear(self, clear_tmp=True):
        """Remove every variable and sub-namespace; with ``clear_tmp`` also the library's
        ``tmp_*`` files in ``temp_dir`` (beware if several namespaces share ``temp_dir``)."""
        for d in [d for d in self.base_path.iterdir() if d.is_dir() if d != self.temp_dir]:
            _retry(lambda d=d: shutil.rmtree(d))
        for f in [f for f in self.base_path.iterdir() if f.is_file()]:
            _retry(f.unlink)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        if clear_tmp:
            self.clear_tmp()

    def clear_tmp(self):
        """Remove the library's ``tmp_*`` files from ``temp_dir``; other files are kept."""
        for f in [f for f in self.temp_dir.iterdir() if f.is_file() and f.name.startswith("tmp_")]:
            try:
                f.unlink()
            except OSError as e:
                logger.debug("could not remove %s: %s", f, e)

    def _dirname_to_name_type(self, dirname):
        s = (dirname).split(self.sep)
        return (self.sep).join(s[1:]), s[0]

    def names_types(self):
        tmp = [
            self._dirname_to_name_type(d.name)
            for d in self.base_path.iterdir()
            if d.is_dir()
        ]
        return [x for x in tmp if x[1] in self.prefixes]

    def names(self):
        return [x[0] for x in self.names_types()]

    def type(self, name):
        try:
            return [x[1] for x in self.names_types() if x[0] == name][0]
        except IndexError:
            return None

    def variable(self, name, fast=False):
        nt = [x for x in self.names_types() if x[0] == name]
        if len(nt) > 0 and nt[0][1] == self.udict_prefix:
            return FSUDict(
                self.base_path / (self.udict_prefix + self.sep + name),
                self.temp_dir,
                self.serializer,
                fast,
            )

        elif len(nt) > 0 and nt[0][1] == self.list_prefix:
            return FSList(
                self.base_path / (self.list_prefix + self.sep + name),
                self.temp_dir,
                self.serializer,
                fast,
            )

        elif len(nt) > 0 and nt[0][1] == self.namespace_prefix:
            return FSNamespace(
                self.base_path / (self.namespace_prefix + self.sep + name),
                self.temp_dir,
                self.serializer,
            )
        else:
            raise ValueError(f"{name} not available in namespace")

    def __getattr__(self, name):
        # Only reached when normal lookup fails. Private names and lookups on an instance
        # that is not initialised yet (copy, pickle) must raise AttributeError, and so must
        # unknown variables, otherwise hasattr(), copy and IPython completion break.
        if name.startswith("_") or "base_path" not in self.__dict__:
            raise AttributeError(name)
        try:
            return self.variable(name)
        except ValueError as e:
            raise AttributeError(str(e)) from None


# ---------------------------------------------------------------------------
# Locks
# ---------------------------------------------------------------------------


class LockingError(Exception):
    def __init__(self, message):
        super().__init__(message)


def _lock_dir(base_path, lock_name):
    return Path(base_path) / (lock_name + ".lock")


def _write_owner(lock_dir):
    """Write ``lock_dir/owner`` (host, pid, time). Informative and used for expiry."""
    info = {"host": socket.gethostname(), "pid": os.getpid(), "time": time.time(), "iso": timestamp()}
    try:
        with open(lock_dir / "owner", "w", encoding="utf-8") as f:
            json.dump(info, f)
    except OSError as e:
        logger.debug("could not write lock owner in %s: %s", lock_dir, e)


def _read_owner(lock_dir):
    try:
        with open(lock_dir / "owner", "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _lock_age(lock_dir):
    """Seconds since the lock was taken (mtime of ``owner``, else of the directory)."""
    for p in (lock_dir / "owner", lock_dir):
        try:
            return time.time() - p.stat().st_mtime
        except OSError:
            continue
    return None


def _break_stale_lock(lock_dir, max_age):
    """Remove ``lock_dir`` if older than ``max_age`` seconds. Returns True if it was broken.

    The directory is first renamed to ``<name>.stale-<uuid>`` (atomic, so only one of
    several waiters breaks it) and then deleted.
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


def acquire_lock(base_path, lock_name, timeout=-1.0, watchdog_timeout=19, wait=(0.0, 0.0), max_age=None):
    """Take the directory lock ``base_path/<lock_name>.lock`` (``os.mkdir`` is atomic).

    timeout: seconds to wait (< 0 = forever, 0 = try once).
    watchdog_timeout: seconds to wait for a filesystem event before retrying blindly.
    wait: (min, max) random seconds to sleep after an event, spreading competing waiters.
    max_age: if given, a lock older than this many seconds is considered abandoned by a
        dead process and is broken (see ``_break_stale_lock``). Use minutes, not seconds,
        and keep the clocks of the hosts reasonably synchronised. None (default): never.
    Raises LockingError on timeout.
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
        # Windows reports a removed directory as a file deletion, so is_directory is not
        # checked. A "moved" event is a stale lock being broken by another waiter.
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
    """Release the lock. A lock that is already gone (broken as stale) only logs a warning."""
    lock_dir = _lock_dir(base_path, lock_name)
    try:
        _retry(lambda: shutil.rmtree(lock_dir))
    except FileNotFoundError:
        logger.warning("lock %s in %s was already gone (broken as stale or released twice)", lock_name, base_path)
        return
    logger.debug("%s unset in %s", lock_name, base_path)


@contextmanager
def lock_context(base_path, lock_name, timeout=60, watchdog_timeout=19, wait=(0.0, 0.0), max_age=None):
    """``with lock_context(path, "name"): ...`` — see ``acquire_lock`` for the parameters."""
    acquire_lock(base_path, lock_name, timeout, watchdog_timeout, wait, max_age)
    try:
        yield
    finally:
        release_lock(base_path, lock_name)
