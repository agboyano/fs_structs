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
job = queue.pop_left(timeout=60)           # safe with several consumers

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
  gets a given key.
- **Exclusive locks.** `acquire_lock` / `lock_context` create a directory with `os.mkdir`,
  which is atomic on local disks, SMB and NFS. Waiters wake up on the filesystem event of
  the release, with a timeout as fallback. With `max_age` a lock left behind by a dead
  process is broken (renamed, then removed) once it is older than that many seconds.
- **Exactly-once queue consumption.** `FSList.pop_left` takes a lock inside the list
  directory, so several consumers on several hosts never take the same element.
  Producers do not lock: the atomic rename is enough.
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
- A namespace name must have one type. If `ud_name` and `li_name` both exist (a race between
  processes, or a directory made by hand), `ns.variable("name")` and `ns.name` raise
  `ValueError`; remove one of the directories.
- `pickle` and `joblib` (the default) execute code when loading. Do not use them on a
  directory writable by untrusted parties; `json_serializer` is available.
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
