# fs_structs

Filesystem data structures that make persistent data management easy, let distributed
processes that only share a directory or a filesystem share data and state, and communicate
with each other. The library works on Windows and Linux machines.

Three structures and one lock, all stored as plain files and directories on a local disk or
a network share (SMB, NFS). No server, no database: files, atomic renames and directory locks.

- `FSUDict`: an **unordered dict**, one file per key.
- `FSList`: a list that can be used as a **FIFO queue** shared by several processes.
- `FSNamespace`: a directory of named dicts, lists and sub-namespaces.
- `lock_context`: one process at a time, on any machine.
- `fs_structs.fslist_simple.FSListSimple`: the previous `FSList`, built on `FSUDict`. Same
  behaviour, other file names; kept to read lists written by versions before 0.0.5.

Every public class and function has runnable examples in its docstring: try
`help(fs_structs.structs.FSUDict)`.

```python
from fs_structs.structs import FSNamespace, lock_context

ns = FSNamespace("/mnt/share/jobs")        # temp files go to /mnt/share/jobs/tmp

prices = ns.udict("prices")                # dict: one file per key
prices[("AAPL", "2026-01-02")] = 187.5
print(prices[("AAPL", "2026-01-02")], len(prices))

queue = ns.list("requests")                # list / FIFO queue
queue.append({"id": 1, "ticker": "AAPL"})
job = queue.pop_left()                     # safe with several consumers, no lock

today = ns.list("today", clean=True)       # emptied (clear()) before it is returned
ns.namespace("config").udict("user")["name"] = "Federico"   # chained: created on first use

with lock_context(prices.base_path, "rebuild", timeout=60, max_age=600):
    ...                                    # exclusive section across hosts

print(ns.names_types())                    # [('prices', 'ud'), ('requests', 'li')]
print(ns.prices[("AAPL", "2026-01-02")])   # variables are attributes too
```

## Installation

```
pip install git+https://github.com/agboyano/fs_structs
```

Requires Python 3.9+, [joblib](https://joblib.readthedocs.io/) and
[watchdog](https://python-watchdog.readthedocs.io/). Tested on Windows and Linux.

## What it guarantees

- **Atomic writes.** `FSUDict.__setitem__` writes to `temp_dir` and renames onto the final
  name (`os.replace`). Readers see the previous value or the new one, never a partial file.
  `pop` renames the file away before reading it, so only one of several concurrent callers
  gets a given key. The value is flushed to disk (`fsync`) before the rename, but the rename
  itself is not: a crash or power loss can lose the most recent writes, leaving an orphan
  temp file, but can never leave a corrupt value. `fast=True` skips both the rename and
  the fsync: about ten times faster on a local disk, but a reader or a crash can see a
  partial file.
- **Exclusive locks.** `acquire_lock` / `lock_context` create a directory with `os.mkdir`,
  which is atomic on local disks, SMB and NFS. Waiters wake up on the filesystem event of
  the release, with a timeout as fallback. With `max_age` a lock left behind by a dead
  process is broken (renamed, then removed) once it is older than that many seconds.
- **Exactly-once queue consumption.** `FSList.pop_left` renames the file of the first
  element into `temp_dir`, reads it and deletes it. On Linux only one of several consumers
  can win the rename. On Windows and on SMB shares the rename goes through a file handle
  and two consumers can both succeed, the second moving the file away from the first; so a
  consumer whose copy has vanished when it opens it, or when it deletes it after reading,
  gives the element up. Exactly one consumer keeps each element, on one host or on several,
  without a lock; a consumer that loses takes the next element. `FSUDict.pop` follows the
  same protocol. Producers do not lock either: the atomic rename of the write is enough.
  (`FSListSimple.pop_left`, the previous implementation, takes a directory lock as well.)
- **Tolerance to transient errors.** `PermissionError` (Windows: a file open elsewhere) and
  transient network errors are retried a few times with a short random wait. Foreign files
  in a data directory (`Thumbs.db`, `desktop.ini`, orphan temp files) are ignored.

## Network shares: SMB / Samba and NFS

The library is made for a directory shared by several machines. What matters is that the
three operations it relies on are executed by the file server, so they are atomic for every
client:

| Operation | Used for | SMB / Samba | NFS |
|---|---|---|---|
| `os.mkdir` | locks | atomic | atomic |
| `os.replace` (rename on the same share) | atomic writes, `pop` | atomic | atomic |
| change notifications | waking up waiters | Windows clients: yes, from the server. Linux clients: no, polling is used | no, polling is used |

**Windows client on an SMB share: verified.** The whole test suite, including the
multi-process queue and lock tests and the "lock wakes up on release" test, passes with
`FS_STRUCTS_TEST_ROOT` on a Samba/SMB share.

**Linux client on a CIFS or NFS mount.** The kernel does not report changes made by other
machines, so the library detects the mount type in `/proc/mounts` and waits by polling
(one second interval). Correctness is the same; a wake-up can take up to one second longer.
Force polling with `FS_STRUCTS_POLLING=1` if your mount type is not detected. This path is
unit-tested with a simulated mounts table but has not yet been run against a real Samba
server from Linux: running the suite there with `FS_STRUCTS_TEST_ROOT` would confirm it.

**On any share.** Keep `temp_dir` on the same share as the data (the default is). Transient
client errors are retried. Notifications can be lost, so every wait has a timeout and
re-checks the state afterwards. Lock expiry (`max_age`) compares the server-assigned file
time with the local clock: keep clocks in sync and use minutes, not seconds.

## Requirements and limitations

- `temp_dir` must be on the **same volume** as the data (a rename across volumes is not
  atomic and fails; the library raises `ValueError`). The default `base_path/tmp` is fine.
- **Linux on network mounts:** inotify does not see changes made by other hosts on NFS/CIFS,
  so paths on such mounts are watched with watchdog's `PollingObserver` (1 s interval),
  detected from `/proc/mounts`. Force it with `FS_STRUCTS_POLLING=1` or
  `fs_structs.watchdog.FORCE_POLLING = True`. Windows receives SMB notifications natively.
- Notifications can be lost on network filesystems. Every wait has a timeout and the state
  is re-checked afterwards, so a lost event costs latency, never correctness.
- `max_age` compares the lock's mtime with the local clock: keep host clocks reasonably in
  sync and use minutes, not seconds, or a slow but alive holder may lose its lock.
- **FIFO order is per process.** List keys are `(time_ns, random)` from each writer's clock;
  the order of elements appended by different hosts is only as good as their clocks.
- **Keys** must round-trip through `repr` / `ast.literal_eval`: str, int, float, bytes, None,
  tuples, lists, dicts, sets. File names are the hex of `repr(key)` plus the extension, so
  a key is limited to about 125 characters of `repr` (255-character file names). On Windows
  without `LongPathsEnabled` the whole path is limited to 259 characters, so keep
  `base_path` short too. Both cases raise `ValueError` with the sizes involved.
- `FSList` lists and sorts the directory on every indexed access: fine for queues and small
  lists, not for large random-access sequences. `items()` returns `(value, key)` pairs and
  slice assignment appends surplus values instead of inserting them.
- `FSList` (0.0.5 and later) names its files differently from `FSListSimple` (the `FSList`
  of 0.0.4 and earlier): the two classes do not see each other's elements. Drain or
  `clear()` a queue written by an old version before using the new class, or read it with
  `fs_structs.fslist_simple.FSListSimple`. `pop_left` keeps the `timeout`,
  `watchdog_timeout` and `max_age` arguments for compatibility but ignores them: there is
  no lock, and `LockingError` is never raised.
- `clear()` (and `clean=True`) is an administrative operation: call it only when no other
  process is using the structure, for example when a job starts. It is not atomic: a value
  written at that moment may survive or be deleted, and `FSNamespace.clear()` also removes
  temporary files in the shared `temp_dir`. Stored values are never corrupted. Files are
  deleted in parallel (`fs_structs.structs.CLEAR_THREADS`, 16 threads by default), which
  on a network share divides the time by roughly that number.
- A namespace name must have one type. If `ud_name` and `li_name` both exist (a race between
  processes, or a directory made by hand), `ns.variable("name")` and `ns.name` raise
  `ValueError`; remove one of the directories.
- `pickle` and `joblib` (the default) execute code when loading. Do not use them on a
  directory writable by untrusted parties; `json_serializer` is available. The three
  serializers accept parameters (`JsonSerializer(indent=2)`, `JoblibSerializer(compress=3)`,
  `PickleSerializer(protocol=2)`); another format is a subclass of `Serializer` with two
  methods (see `help(fs_structs.structs.Serializer)`).
- Directories synchronised with a delay between hosts (OneDrive, Dropbox and the like) are
  **not supported**: neither rename nor mkdir is atomic across hosts there.

## Tests

```
pip install -e ".[test]"
pytest -q
```

To run the same suite on a network share, point `FS_STRUCTS_TEST_ROOT` at a directory there:

```
FS_STRUCTS_TEST_ROOT=/mnt/share/fs_structs_tests pytest -q      # Linux
set FS_STRUCTS_TEST_ROOT=Z:\fs_structs_tests && pytest -q         # Windows
```

The concurrency tests start real processes (`multiprocessing`, `spawn`) that append to and
consume from one queue and increment a counter under a lock. CI runs the suite on Ubuntu
and Windows.
