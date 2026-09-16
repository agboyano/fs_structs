# AGENTS.md

Guidance for coding agents (and humans) working on `fs_structs`.

## What this library is

Filesystem data structures that make persistent data management easy, let distributed
processes that only share a directory or a filesystem share data and state, and communicate
with each other. The library works on Windows and Linux machines.

- `fs_structs/structs.py`: `FSUDict` (unordered dict, one file per key), `FSList` (list that
  can be used as a FIFO queue), `FSNamespace` (directory of named dicts, lists and
  sub-namespaces), directory locks (`acquire_lock`, `release_lock`, `lock_context`) and the
  serializers (`joblib`, `pickle`, `json`).
- `fs_structs/watchdog.py`: waiting for filesystem events with the `watchdog` package,
  with polling on Linux network mounts.
- `tests/`: pytest suite. `conftest.py` provides the `root` fixture (a fresh directory,
  `tmp_path` or `$FS_STRUCTS_TEST_ROOT`).
- `examples/fs_structs_tests.ipynb`: notebook walkthrough. Keep its outputs empty.
- `.github/workflows/tests.yml`: CI on ubuntu-latest and windows-latest, Python 3.9 and 3.13.

README.md documents the guarantees, requirements and limitations. Read it first.

## Guarantees you must not break

- **Atomic writes**: a value is written to `temp_dir` and renamed onto its final name with
  `os.replace`. Readers never see a partial file. `temp_dir` must stay on the same volume.
- **Exclusive locks**: `os.mkdir` of the `.lock` directory is the lock. It works on local
  disks, SMB/Samba and NFS. The `owner` file inside is informative and used by `max_age`.
- **Exactly-once `pop_left`**: consumers on any machine take the lock inside the list
  directory; `FSUDict.pop` renames the file away before reading it.
- **Compatibility**: the public API is used by other projects (positional arguments). New
  parameters go last, with a default that keeps the current behaviour. Do not rename or
  remove public names, and do not change the file-name scheme (`hex(repr(key)) + "." + ext`)
  without a migration.
- **Python 3.9**: no syntax or stdlib features newer than 3.9 (no `X | Y` type unions at
  runtime, no `match`).
- **Dependencies**: `joblib` and `watchdog` only. Do not add others without a strong reason.

Out of scope, on purpose: directories synchronised with a delay between hosts (OneDrive,
Dropbox and the like). Do not add code paths for them.

## Documentation conventions

- Google-style docstrings: `Args:`, `Returns:`, `Raises:`, `See Also:`, `Examples:`.
- `Examples:` is mandatory for every public class, method and function. The examples are
  doctests and **run in pytest and CI** (`--doctest-modules`). They must be self-contained
  (`tempfile.mkdtemp()`), deterministic (sort anything unordered) and cross-platform.
  Dunder methods may carry a one-line docstring plus `See Also:` pointing to the class.
- Write for readers whose first language is not English: short sentences, one idea per
  sentence, no idioms.
- Implementation details (atomicity, platform behaviour, trade-offs) go in a
  `# Implementation notes:` comment block right before the `def` / `class`, not in the
  docstring. The docstring is for the user; the comment block is for the maintainer.
- Say what each structure is: `FSUDict` is an **unordered** dict; `FSList` is a list that
  can be used as a **FIFO queue**.
- This is a public repository: nothing about a company, a team or a person; no internal
  paths or server names; notebooks without outputs.

## Coding conventions

- Do not delete code on your own initiative. Code that becomes unused or unreachable is
  left in place, commented and marked as such, and the decision is reported to the owner.
- Keep it simple: prefer a small helper over a new abstraction; prefer the standard library.
- Every file operation that can fail transiently on Windows or on a share goes through
  `_retry` (see `structs.py`). `FileNotFoundError` is never retried.
- New behaviour comes with a test in `tests/` and, when it is public, a doctest.

## How to verify

```
pip install -e ".[test]"          # or: uv pip install -e ".[test]"
pytest -q                         # tests + doctests, local disk
FS_STRUCTS_TEST_ROOT=/mnt/share/x pytest -q     # same suite on a network share (Linux)
set FS_STRUCTS_TEST_ROOT=Z:\x && pytest -q       # same on Windows
python -W error -c "import fs_structs"
```

The concurrency tests start real processes with `multiprocessing` (spawn) and take about
15 s on a local disk and 30 s on a share. All four CI jobs must stay green after a push.

When a change touches locks, renames or the observer choice, also run the test suites of
the projects that depend on this library, if you have them, before reporting completion.

## Reporting

Report what was verified and where (local disk, share, CI), and say plainly what was not
verified. A non-zero test run is never described as success.
