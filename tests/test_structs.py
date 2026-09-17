"""FSUDict, FSList, FSNamespace and locks on a real directory (see conftest.root)."""

import copy
import errno
import random
import threading
import time
from pathlib import Path

import pytest

from fs_structs import structs
from fs_structs.fslist_simple import FSListSimple
from fs_structs.structs import (
    FSList,
    FSNamespace,
    FSUDict,
    JoblibSerializer,
    JsonSerializer,
    LockingError,
    PickleSerializer,
    Serializer,
    acquire_lock,
    joblib_serializer,
    json_serializer,
    lock_context,
    pickle_serializer,
    release_lock,
)


# --------------------------------------------------------------------------- FSUDict


@pytest.mark.parametrize("serializer", [joblib_serializer, pickle_serializer], ids=["joblib", "pickle"])
def test_udict_round_trips_keys_and_values(root, serializer):
    d = FSUDict(root / "d", root / "tmp", serializer=serializer)
    values = {
        "1": 3,
        1: 10,
        "go": "hola",
        (1, 2): [1, 2, (1, 4, 5), "kk"],
        "kk": {"a": 1, "b": [1, 2]},
        None: "none",
        2.5: b"bytes",
        (("nested", 1), -3, "x"): {"x": None},
    }
    for k, v in values.items():
        d[k] = v

    assert len(d) == len(values)
    assert sorted(map(repr, d.keys())) == sorted(map(repr, values))
    assert sorted(map(repr, d)) == sorted(map(repr, values))
    for k, v in values.items():
        assert k in d
        assert d[k] == v
    assert dict(d.items()) == values
    assert sorted(map(repr, d.values())) == sorted(map(repr, values.values()))

    assert d.get("missing", 42) == 42
    assert "missing" not in d
    with pytest.raises(KeyError):
        d["missing"]

    del d["go"]
    assert "go" not in d
    with pytest.raises(KeyError):
        del d["go"]

    assert d.pop(1) == 10
    assert 1 not in d
    with pytest.raises(KeyError):
        d.pop(1)

    d.update({"u1": 1, "u2": 2})
    d.update([("u3", 3)])
    assert d["u1"] == 1 and d["u3"] == 3

    d.clear()
    assert len(d) == 0
    assert d.base_path.is_dir() and d.temp_dir.is_dir()


@pytest.mark.parametrize("tmp_inside", [False, True], ids=["tmp_outside", "tmp_inside"])
def test_udict_clear_empties_in_place(root, tmp_inside):
    temp_dir = root / "d" / "tmp" if tmp_inside else root / "tmp"
    d = FSUDict(root / "d", temp_dir)
    d.update({i: i for i in range(200)})
    acquire_lock(d.base_path, "l")
    (d.base_path / "stray.txt").write_bytes(b"x")
    (d.base_path / "other" / "deep").mkdir(parents=True)
    (d.base_path / "other" / "deep" / "f").write_bytes(b"x")

    d.clear()

    assert len(d) == 0
    assert d.base_path.is_dir() and d.temp_dir.is_dir()
    left = [p.name for p in d.base_path.iterdir()]
    assert left == (["tmp"] if tmp_inside else [])


def test_udict_dict_as_key(root):
    d = FSUDict(root / "d", root / "tmp")
    key = {"adad": 1233, (1, 3, 4): [1, 2, 3, 4], 3: 1}
    d[key] = 1234
    assert d[key] == 1234
    assert d.keys() == [key]


def test_udict_fast_mode(root, monkeypatch):
    fsyncs = []
    monkeypatch.setattr(structs.os, "fsync", lambda fd: fsyncs.append(fd))

    d = FSUDict(root / "d", root / "tmp", fast=True)
    d["k"] = [1, 2]
    assert fsyncs == []  # fast mode: no fsync
    assert d["k"] == [1, 2]
    assert d.pop("k") == [1, 2]
    assert "k" not in d
    assert list((root / "tmp").iterdir()) == []

    safe = FSUDict(root / "s", root / "tmp")
    safe["k"] = [1, 2]
    assert len(fsyncs) == 1  # default mode: one fsync per write
    assert safe["k"] == [1, 2]


def test_json_serializer_reads_utf8_on_every_platform(root):
    d = FSUDict(root / "d", root / "tmp", serializer=json_serializer)
    d["k"] = {"text": "añá € 漢字"}
    assert d["k"] == {"text": "añá € 漢字"}


def test_udict_ignores_foreign_files_and_directories(root):
    d = FSUDict(root / "d", root / "tmp")
    d["a"] = 1
    d["b"] = 2
    for name in ["Thumbs.db", "desktop.ini", "tmp_deadbeef", "zz.jbl", "-not-hex-.jbl", "x.pkl"]:
        (d.base_path / name).write_bytes(b"x")
    (d.base_path / "some_lock.lock").mkdir()

    assert sorted(d.keys()) == ["a", "b"]
    assert len(d) == 2
    assert dict(d.items()) == {"a": 1, "b": 2}
    assert sorted(d.values()) == [1, 2]


def test_temp_dir_on_another_volume_gives_a_clear_error(root, monkeypatch):
    d = FSUDict(root / "d", root / "tmp")

    def cross_device(self, target):
        raise OSError(errno.EXDEV, "Invalid cross-device link")

    monkeypatch.setattr(Path, "replace", cross_device)
    with pytest.raises(ValueError, match="same volume"):
        d["k"] = 1
    assert list((root / "tmp").iterdir()) == []  # the temp file is cleaned up


def test_long_key_gives_a_clear_error(root):
    d = FSUDict(root / "d", root / "tmp")
    d["y" * 40] = 1  # 40 chars of key -> 88 chars of file name: fine everywhere
    assert d["y" * 40] == 1
    with pytest.raises(ValueError, match="too long"):
        d["x" * 200] = 1  # 404 chars of file name


@pytest.mark.skipif(structs._windows_long_paths_enabled(), reason="only without Windows long paths")
def test_long_path_on_windows_gives_a_clear_error(root):
    d = FSUDict(root / "d", root / "tmp")
    key = "z" * 110  # 228 chars of file name: valid name, but the full path exceeds 259
    with pytest.raises(ValueError, match="LongPathsEnabled"):
        d[key] = 1


# --------------------------------------------------------------------------- FSList

# FSList (fast, own files) and FSListSimple (previous, on FSUDict) must behave the same.
LIST_CLASSES = pytest.mark.parametrize("cls", [FSList, FSListSimple], ids=["FSList", "FSListSimple"])


@LIST_CLASSES
def test_list_behaves_like_a_python_list(root, cls):
    lst = cls(root / "l", root / "tmp")
    ref = []

    for i in range(20):
        lst.append(i)
        ref.append(i)
    assert lst.values() == ref
    assert list(lst) == ref
    assert lst.copy() == ref
    assert len(lst) == 20
    assert lst[0] == ref[0] and lst[-1] == ref[-1]
    assert lst[2:5] == ref[2:5]
    assert lst[::7] == ref[::7]

    lst[0] = -1
    ref[0] = -1
    lst[5] = 12
    ref[5] = 12
    assert lst.values() == ref

    lst[1:3] = [100, 101]
    ref[1:3] = [100, 101]
    assert lst.values() == ref

    for _ in range(3):
        assert lst.pop(0) == ref.pop(0)
    assert lst.pop() == ref.pop()
    assert lst.pop(-2) == ref.pop(-2)
    assert lst.values() == ref

    for i in range(5):
        lst.insert(0, 100 + i)
        ref.insert(0, 100 + i)
    for i in range(3):
        lst.insert(i + 1, 200 + i)
        ref.insert(i + 1, 200 + i)
    lst.insert(len(lst), 300)
    ref.insert(len(ref), 300)
    lst.insert(10 ** 6, 301)
    ref.insert(10 ** 6, 301)
    assert lst.values() == ref

    del lst[3]
    del ref[3]
    del lst[-1]
    del ref[-1]
    assert lst.values() == ref

    assert 300 in lst
    assert 12345 not in lst

    lst.extend([7, 8])
    ref.extend([7, 8])
    assert lst.values() == ref

    lst.clear()
    assert len(lst) == 0


def test_list_delitem_uses_the_sorted_order(root, monkeypatch):
    lst = FSListSimple(root / "l", root / "tmp")
    lst.extend(["a", "b", "c"])

    original_keys = FSUDict.keys
    monkeypatch.setattr(FSUDict, "keys", lambda self: list(reversed(original_keys(self))))

    del lst[0]
    assert lst.values() == ["b", "c"]


@LIST_CLASSES
def test_list_items_are_value_key_pairs_in_order(root, cls):
    lst = cls(root / "l", root / "tmp")
    lst.extend([10, 20])
    items = lst.items()
    assert [v for v, _ in items] == [10, 20]
    assert [k for _, k in items] == lst.keys()


@LIST_CLASSES
def test_pop_left_is_fifo_and_raises_when_empty(root, cls):
    lst = cls(root / "l", root / "tmp")
    lst.extend([1, 2, 3])

    assert [lst.pop_left(timeout=5) for _ in range(3)] == [1, 2, 3]
    with pytest.raises(IndexError):
        lst.pop_left(timeout=5)
    assert len(lst) == 0
    assert not (lst.base_path / "pop_left_lock.lock").exists()

    fast = cls(root / "f", root / "tmp", fast=True)
    fast.extend([1, 2])
    assert fast.pop_left() == 1
    with pytest.raises(IndexError):
        fast.pop_left()
        fast.pop_left()


def test_fslist_and_fslistsimple_agree(root):
    """The same scripted operations give the same values on both classes and on a list."""
    a = FSList(root / "a", root / "tmp")
    b = FSListSimple(root / "b", root / "tmp")
    ref = []
    rng = random.Random(20260917)
    ops = ["append", "insert", "setitem", "pop", "pop0", "pop_left", "delitem", "extend", "setslice"]

    for step in range(80):
        op = rng.choice(ops)
        n = len(ref)
        if op == "append":
            v = step
            for lst in (a, b, ref):
                lst.append(v)
        elif op == "extend":
            vs = [step, -step]
            for lst in (a, b, ref):
                lst.extend(vs)
        elif op == "insert":
            i = rng.randint(0, n)
            for lst in (a, b, ref):
                lst.insert(i, 1000 + step)
        elif n == 0:
            for lst in (a, b):
                with pytest.raises(IndexError):
                    lst.pop_left() if op == "pop_left" else lst.pop()
            continue
        elif op == "setitem":
            i = rng.randint(-n, n - 1)
            for lst in (a, b, ref):
                lst[i] = 2000 + step
        elif op == "setslice":
            i = rng.randint(0, n)
            j = rng.randint(i, n)
            vs = [3000 + step + k for k in range(j - i)]  # same length: list semantics apply
            for lst in (a, b, ref):
                lst[i:j] = vs
        elif op == "pop":
            i = rng.randint(-n, n - 1)
            assert a.pop(i) == b.pop(i) == ref.pop(i)
        elif op == "pop0":
            assert a.pop(0) == b.pop(0) == ref.pop(0)
        elif op == "pop_left":
            assert a.pop_left() == b.pop_left(timeout=5) == ref.pop(0)
        elif op == "delitem":
            i = rng.randint(-n, n - 1)
            for lst in (a, b, ref):
                del lst[i]

        assert a.values() == b.values() == ref, (step, op)
        assert len(a) == len(b) == len(ref)
        assert [k for _, k in a.items()] == a.keys()
        assert [v for v, _ in a.items()] == ref
        assert list(a) == a.copy() == a[:] == ref
        assert (step in a) == (step in b) == (step in ref)


def test_fslist_names_sort_like_keys():
    gen = structs.TimeOrderedTuple()
    a, b = gen.new_rear_tuple(), gen.new_rear_tuple()
    mid = gen.new_mid_tuple(a, b)
    keys = [a, b, mid, gen.new_mid_tuple(a, mid), gen.new_front_tuple(), gen.new_front_tuple()]
    keys += [(0,), (-1,), (-1, 5), (5, 3), (5, 3, 1), (5, 3, -(1 << 63)), ((1 << 63) - 1,), (-(1 << 63), 7)]

    for key in keys:
        assert structs._decode_key(structs._encode_key(key)) == key

    names = [structs._encode_key(k) + ".jbl" for k in keys]
    decoded_in_name_order = [FSList._key_of(n) for n in sorted(names)]
    assert decoded_in_name_order == sorted(keys)

    with pytest.raises(ValueError):
        structs._encode_key((1 << 63,))


def test_fslist_ignores_foreign_files(root):
    lst = FSList(root / "l", root / "tmp")
    lst.extend([1, 2])
    acquire_lock(lst.base_path, "pop_left_lock")
    (lst.base_path / "Thumbs.db").write_bytes(b"x")
    (lst.base_path / ("f" * 15 + ".jbl")).write_bytes(b"x")  # 15 hex digits, not 16
    (lst.base_path / ("f" * 16 + ".pkl")).write_bytes(b"x")  # other extension
    (lst.base_path / ("g" * 16 + ".jbl")).write_bytes(b"x")  # not hex

    assert len(lst) == 2
    assert lst.values() == [1, 2]
    assert lst.pop_left() == 1
    release_lock(lst.base_path, "pop_left_lock")


def test_pop_left_skips_elements_taken_by_others(root, monkeypatch):
    lst = FSList(root / "l", root / "tmp")
    lst.extend([1, 2, 3])
    original = FSList._take
    lost = []

    def take(self, name):
        if not lost:  # the first attempt loses the race: another consumer took the file
            lost.append(name)
            raise KeyError(name)
        return original(self, name)

    monkeypatch.setattr(FSList, "_take", take)
    assert lst.pop_left() == 2
    assert [lst.pop_left(), lst.pop_left()] == [1, 3]  # 1 was not really taken, so it is still there


def test_pop_left_lists_again_when_every_listed_element_is_gone(root, monkeypatch):
    lst = FSList(root / "l", root / "tmp")
    lst.append("real")
    stale = structs._encode_key((1, 1)) + ".jbl"  # a name that no longer exists on disk
    original = FSList._names
    calls = []

    def names(self):
        calls.append(1)
        return [stale] if len(calls) == 1 else original(self)

    monkeypatch.setattr(FSList, "_names", names)
    assert lst.pop_left() == "real"
    assert len(calls) == 2


# --------------------------------------------------------------------------- FSNamespace


def test_namespace_types_and_reopening(root):
    ns = FSNamespace(root / "ns")
    ns.udict("a")["k"] = 1
    ns.list("l").append(1)
    ns.namespace("sub").udict("inner")["x"] = 2

    assert sorted(ns.names_types()) == [("a", "ud"), ("l", "li"), ("sub", "ns")]
    assert sorted(ns.names()) == ["a", "l", "sub"]
    assert ns.type("a") == "ud" and ns.type("l") == "li" and ns.type("sub") == "ns"
    assert ns.type("missing") is None

    # Reopening with the right type works, including sub-namespaces.
    assert ns.udict("a")["k"] == 1
    assert ns.list("l")[0] == 1
    assert isinstance(ns.namespace("sub"), FSNamespace)
    assert ns.namespace("sub").udict("inner")["x"] == 2
    assert ns.variable("sub").inner["x"] == 2
    assert ns.sub.inner["x"] == 2

    # Reopening with another type is an error.
    with pytest.raises(ValueError):
        ns.namespace("l")
    with pytest.raises(ValueError):
        ns.udict("l")
    with pytest.raises(ValueError):
        ns.list("a")
    with pytest.raises(ValueError):
        ns.variable("missing")

    assert ns.temp_dir == ns.base_path / "tmp"
    assert ns.namespace("sub").temp_dir == ns.temp_dir


def test_namespace_getattr_is_well_behaved(root):
    ns = FSNamespace(root / "ns")
    ns.udict("a")["k"] = 1

    assert ns.a["k"] == 1
    assert not hasattr(ns, "missing")
    with pytest.raises(AttributeError):
        ns._private
    with pytest.raises(AttributeError):
        ns.missing

    clone = copy.copy(ns)
    assert isinstance(clone, FSNamespace)
    assert clone.a["k"] == 1


def test_namespace_clear_and_clear_tmp(root):
    ns = FSNamespace(root / "ns")
    ns.udict("a")["k"] = 1
    ns.list("l").append(1)
    (ns.temp_dir / "tmp_orphan").write_bytes(b"x")
    (ns.temp_dir / "keep.txt").write_bytes(b"x")
    (ns.base_path / "stray.txt").write_bytes(b"x")

    ns.clear(clear_tmp=False)
    assert ns.names() == []
    assert not (ns.base_path / "stray.txt").exists()
    assert (ns.temp_dir / "tmp_orphan").exists()

    ns.clear(clear_tmp=True)
    assert not (ns.temp_dir / "tmp_orphan").exists()
    assert (ns.temp_dir / "keep.txt").exists()
    assert ns.temp_dir.is_dir()


# --------------------------------------------------------------------------- locks


def test_lock_is_exclusive_and_writes_its_owner(root):
    with lock_context(root, "l"):
        assert (root / "l.lock" / "owner").is_file()
        with pytest.raises(LockingError):
            acquire_lock(root, "l", timeout=0)
        with pytest.raises(LockingError):
            acquire_lock(root, "l", timeout=0.3, watchdog_timeout=0.1)
    assert not (root / "l.lock").exists()

    # Reacquiring after release works, and a second release only warns.
    assert acquire_lock(root, "l", timeout=0)
    release_lock(root, "l")
    release_lock(root, "l")


def test_lock_wakes_up_when_released(root):
    assert acquire_lock(root, "l", timeout=0)
    threading.Timer(0.5, release_lock, args=(root, "l")).start()

    t0 = time.perf_counter()
    assert acquire_lock(root, "l", timeout=10, watchdog_timeout=5)
    elapsed = time.perf_counter() - t0
    release_lock(root, "l")

    # With the filesystem event the wait is ~0.5 s; without it, the whole watchdog_timeout.
    assert elapsed < 3.0, f"lock release event not received, waited {elapsed:.1f}s"


def test_lock_context_releases_on_error(root):
    with pytest.raises(RuntimeError):
        with lock_context(root, "l", timeout=0):
            raise RuntimeError("boom")
    assert not (root / "l.lock").exists()


def test_release_of_an_old_style_empty_lock(root):
    (root / "old.lock").mkdir()  # created by a previous version: no owner file
    release_lock(root, "old")
    assert not (root / "old.lock").exists()


def test_sleep_helper():
    t0 = time.perf_counter()
    structs.sleep(0.01)
    structs.sleep(0.01, 0.02)
    assert time.perf_counter() - t0 >= 0.02


# --------------------------------------------------------------------------- clean


def test_clean_argument_empties_the_structure(root):
    FSUDict(root / "d", root / "tmp")["k"] = 1
    FSList(root / "l", root / "tmp").append(1)
    ns = FSNamespace(root / "ns")
    ns.udict("a")["k"] = 1

    # Default: data is kept.
    assert FSUDict(root / "d", root / "tmp")["k"] == 1
    assert FSList(root / "l", root / "tmp").values() == [1]
    assert FSNamespace(root / "ns").names() == ["a"]

    # clean=True: emptied before it is returned.
    assert len(FSUDict(root / "d", root / "tmp", clean=True)) == 0
    assert len(FSList(root / "l", root / "tmp", clean=True)) == 0
    assert FSNamespace(root / "ns", clean=True).names() == []


def test_namespace_methods_pass_clean_and_only_touch_their_variable(root):
    ns = FSNamespace(root / "ns")
    ns.udict("a")["k"] = 1
    ns.list("l").append(1)
    ns.namespace("s").udict("inner")["k"] = 1
    ns.udict("keep")["k"] = 1

    assert len(ns.udict("a", clean=True)) == 0
    assert len(ns.list("l", clean=True)) == 0
    assert ns.namespace("s", clean=True).names() == []
    assert ns.keep["k"] == 1  # a sibling is not touched

    ns.udict("a")["k"] = 2
    assert len(ns.variable("a", clean=True)) == 0
    assert ns.keep["k"] == 1
    assert sorted(ns.names()) == ["a", "keep", "l", "s"]  # the variables still exist, empty


def test_chained_namespaces_and_structures(root):
    ns = FSNamespace(root / "ns")
    ns.namespace("config").udict("user")["name"] = "Federico"
    assert ns.config.user["name"] == "Federico"

    ns.config.user["role"] = "admin"
    ns.namespace("config").udict("user", clean=True)["name"] = "Ana"
    assert sorted(ns.config.user.items()) == [("name", "Ana")]

    ns.namespace("jobs", clean=True).list("queue").append("job-1")
    assert ns.jobs.queue.values() == ["job-1"]


def test_duplicated_name_raises_on_variable_and_attribute(root):
    ns = FSNamespace(root / "ns")
    ns.udict("dup")["k"] = 1
    ns.udict("ok")["k"] = 2
    (ns.base_path / "li_dup").mkdir()  # the same name as a list, made by hand

    with pytest.raises(ValueError, match="several types"):
        ns.variable("dup")
    with pytest.raises(ValueError, match="several types"):
        ns.dup

    assert ns.type("dup") in ("ud", "li")  # unchanged: first entry found
    assert ns.ok["k"] == 2
    assert not hasattr(ns, "missing")


# --------------------------------------------------------------------------- serializers


def test_custom_serializer_subclass(root):
    class TextSerializer(Serializer):
        extension = "txt"
        binary = False

        def _write(self, value, f):
            f.write(value)

        def _read(self, f):
            return f.read()

    d = FSUDict(root / "d", root / "tmp", serializer=TextSerializer())
    d["k"] = "añá €"
    assert d["k"] == "añá €"
    assert [p.suffix for p in d.base_path.iterdir()] == [".txt"]


def test_serializer_parameters(root):
    pretty = JsonSerializer(indent=2, ensure_ascii=False)
    pretty.dump({"name": "Málaga"}, root / "v.json")
    text = (root / "v.json").read_text(encoding="utf-8")
    assert "\n" in text and "Málaga" in text
    assert pretty.load(root / "v.json") == {"name": "Málaga"}

    for serializer in (JoblibSerializer(compress=3), PickleSerializer(protocol=2), JsonSerializer()):
        d = FSUDict(root / serializer.extension, root / "tmp", serializer=serializer)
        d["k"] = {"a": [1, 2]}
        assert d["k"] == {"a": [1, 2]}


def test_default_serializers_keep_the_old_file_format(root):
    """Byte compatibility with files written by the previous function-based serializers."""
    import json
    import pickle

    json_serializer.dump({"city": "Málaga"}, root / "v.json")
    assert (root / "v.json").read_bytes() == json.dumps({"city": "Málaga"}).encode("utf-8")

    pickle_serializer.dump((1, 2), root / "v.pkl")
    assert (root / "v.pkl").read_bytes() == pickle.dumps((1, 2), protocol=pickle.HIGHEST_PROTOCOL)

    assert (joblib_serializer.extension, pickle_serializer.extension, json_serializer.extension) == ("jbl", "pkl", "json")
