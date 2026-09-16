"""Transient errors, stale locks and observer choice."""

import json
import os
import time
from pathlib import Path

import pytest

from fs_structs import structs
from fs_structs import watchdog as fsw
from fs_structs.structs import FSUDict, LockingError, acquire_lock, release_lock


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(structs, "sleep", lambda *a: None)


# --------------------------------------------------------------------------- retries


def test_setitem_retries_transient_permission_errors(root, monkeypatch, no_sleep):
    d = FSUDict(root / "d", root / "tmp")
    original = Path.replace
    calls = {"n": 0}

    def flaky(self, target):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise PermissionError(13, "The process cannot access the file")
        return original(self, target)

    monkeypatch.setattr(Path, "replace", flaky)
    d["k"] = 1
    assert d["k"] == 1
    assert calls["n"] == 3
    assert list((root / "tmp").iterdir()) == []


def test_persistent_permission_error_is_raised_and_temp_file_removed(root, monkeypatch, no_sleep):
    d = FSUDict(root / "d", root / "tmp")

    def always_busy(self, target):
        raise PermissionError(13, "The process cannot access the file")

    monkeypatch.setattr(Path, "replace", always_busy)
    with pytest.raises(PermissionError):
        d["k"] = 1
    assert list((root / "tmp").iterdir()) == []


def test_getitem_retries_transient_errors_but_not_missing_keys(root, monkeypatch, no_sleep):
    d = FSUDict(root / "d", root / "tmp")
    d["k"] = 1
    original = d.load
    calls = {"n": 0, "busy_once": True}

    def flaky(path):
        calls["n"] += 1
        if calls["busy_once"]:
            calls["busy_once"] = False
            raise PermissionError(13, "busy")
        return original(path)

    d.load = flaky
    assert d["k"] == 1
    assert calls["n"] == 2

    calls["n"] = 0
    with pytest.raises(KeyError):
        d["missing"]
    assert calls["n"] == 1  # FileNotFoundError is not retried


def test_clear_deletes_the_rest_and_raises_when_one_file_is_busy(root, monkeypatch, no_sleep):
    d = FSUDict(root / "d", root / "tmp")
    d.update({i: i for i in range(50)})
    busy = d.base_path / d._key_to_filename(7)
    original = os.unlink

    def unlink(p, *a, **k):
        if Path(p) == busy:
            raise PermissionError(13, "The process cannot access the file")
        return original(p, *a, **k)

    monkeypatch.setattr(os, "unlink", unlink)
    with pytest.raises(PermissionError):
        d.clear()
    assert d.keys() == [7]


def test_clear_ignores_files_removed_meanwhile(root, monkeypatch, no_sleep):
    d = FSUDict(root / "d", root / "tmp")
    d.update({i: i for i in range(50)})
    raced = d.base_path / d._key_to_filename(7)
    original = os.unlink

    def unlink(p, *a, **k):
        original(p, *a, **k)
        if Path(p) == raced:
            raise FileNotFoundError(2, "No such file")  # another process got there first

    monkeypatch.setattr(os, "unlink", unlink)
    d.clear()
    assert len(d) == 0


def test_transient_network_oserror_is_retried(no_sleep):
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            e = OSError(22, "The semaphore timeout period has expired")
            e.winerror = 121
            raise e
        return "ok"

    assert structs._retry(flaky) == "ok"
    assert calls["n"] == 2


def test_non_transient_oserror_is_not_retried(no_sleep):
    calls = {"n": 0}

    def broken():
        calls["n"] += 1
        raise IsADirectoryError(21, "is a directory")

    with pytest.raises(IsADirectoryError):
        structs._retry(broken)
    assert calls["n"] == 1


# --------------------------------------------------------------------------- stale locks


def _make_old(path, seconds):
    old = time.time() - seconds
    os.utime(path, (old, old))


def test_stale_lock_is_broken_only_when_max_age_is_given(root):
    assert acquire_lock(root, "l", timeout=0)
    _make_old(root / "l.lock" / "owner", 3600)
    _make_old(root / "l.lock", 3600)

    # Default: an abandoned lock is never broken.
    with pytest.raises(LockingError):
        acquire_lock(root, "l", timeout=0)

    # With max_age the lock is broken, removed and taken by the caller.
    assert acquire_lock(root, "l", timeout=0, max_age=600)
    owner = json.loads((root / "l.lock" / "owner").read_text(encoding="utf-8"))
    assert owner["pid"] == os.getpid()
    assert [p.name for p in root.iterdir() if "stale" in p.name] == []

    release_lock(root, "l")
    assert not (root / "l.lock").exists()


def test_fresh_lock_is_not_broken(root):
    assert acquire_lock(root, "l", timeout=0)
    with pytest.raises(LockingError):
        acquire_lock(root, "l", timeout=0, max_age=600)
    assert (root / "l.lock" / "owner").is_file()
    release_lock(root, "l")


def test_old_style_lock_without_owner_file_can_expire(root):
    (root / "l.lock").mkdir()
    _make_old(root / "l.lock", 3600)
    assert acquire_lock(root, "l", timeout=0, max_age=600)
    release_lock(root, "l")


def test_break_stale_lock_loses_the_race_gracefully(root):
    assert structs._break_stale_lock(root / "gone.lock", max_age=1) is False


# --------------------------------------------------------------------------- observer choice

MOUNTS = """rootfs / rootfs rw 0 0
/dev/sda1 / ext4 rw,relatime 0 0
server:/export /mnt/nfs nfs4 rw,vers=4.2 0 0
//srv/share /mnt/smb cifs rw 0 0
//srv/share2 /mnt/with\\040space cifs rw 0 0
tmpfs /run tmpfs rw 0 0
"""


@pytest.fixture
def mounts(tmp_path):
    p = tmp_path / "mounts"
    p.write_text(MOUNTS, encoding="utf-8")
    return str(p)


@pytest.fixture
def auto_polling(monkeypatch):
    monkeypatch.setattr(fsw, "FORCE_POLLING", None)
    monkeypatch.delenv("FS_STRUCTS_POLLING", raising=False)


def test_mount_fstype_picks_the_longest_mount(mounts):
    assert fsw._mount_fstype("/mnt/nfs/queue/x", mounts) == "nfs4"
    assert fsw._mount_fstype("/mnt/nfs", mounts) == "nfs4"
    assert fsw._mount_fstype("/mnt/nfsx", mounts) == "ext4"
    assert fsw._mount_fstype("/mnt/smb/q", mounts) == "cifs"
    assert fsw._mount_fstype("/mnt/with space/q", mounts) == "cifs"
    assert fsw._mount_fstype("/home/x", mounts) == "ext4"
    assert fsw._mount_fstype("/home/x", "/nonexistent/mounts") is None


def test_polling_on_linux_network_mounts_only(mounts, monkeypatch, auto_polling):
    monkeypatch.setattr(fsw.sys, "platform", "linux")
    assert fsw._needs_polling(["/mnt/nfs/queue"], mounts) is True
    assert fsw._needs_polling(["/mnt/smb/q"], mounts) is True
    assert fsw._needs_polling(["/home/x", "/mnt/nfs/q"], mounts) is True
    assert fsw._needs_polling(["/home/x", "/run/y"], mounts) is False

    monkeypatch.setattr(fsw.sys, "platform", "win32")
    assert fsw._needs_polling(["/mnt/nfs/queue"], mounts) is False


def test_polling_overrides(mounts, monkeypatch, auto_polling):
    monkeypatch.setattr(fsw.sys, "platform", "linux")

    monkeypatch.setenv("FS_STRUCTS_POLLING", "1")
    assert fsw._needs_polling(["/home/x"], mounts) is True
    monkeypatch.setenv("FS_STRUCTS_POLLING", "0")
    assert fsw._needs_polling(["/mnt/nfs/q"], mounts) is False

    monkeypatch.setattr(fsw, "FORCE_POLLING", True)
    assert fsw._needs_polling(["/home/x"], mounts) is True
    monkeypatch.setattr(fsw, "FORCE_POLLING", False)
    monkeypatch.setenv("FS_STRUCTS_POLLING", "1")
    assert fsw._needs_polling(["/mnt/nfs/q"], mounts) is False


def test_observer_for_returns_the_requested_kind(tmp_path):
    assert isinstance(fsw._observer_for([tmp_path], polling=True), fsw.PollingObserver)
    native = fsw._observer_for([tmp_path], polling=False)
    assert not isinstance(native, fsw.PollingObserver)


@pytest.mark.parametrize("polling", [False, True])
def test_wait_until_sees_a_created_file(root, polling):
    import threading

    target = root / "hello.txt"
    threading.Timer(0.3, target.write_bytes, args=(b"x",)).start()

    event, event_type, parent, name = fsw.wait_until_file_event(
        [root], ["hello"], ["created"], timeout=10, polling=polling
    )
    assert event is not None
    assert event_type == "created"
    assert name == "hello.txt"
    assert parent == root.resolve()


def test_wait_until_times_out(root):
    t0 = time.perf_counter()
    assert fsw.wait_until([root], timeout=0.3) == (None, None, None, None)
    assert time.perf_counter() - t0 < 5
