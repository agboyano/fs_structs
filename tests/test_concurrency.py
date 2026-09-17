"""Several processes on the same directory: exactly-once consumption and exclusive locks.

Targets are module-level functions so the ``spawn`` start method (the only one on Windows,
and the safest with threads) can import them in the children.
"""

import multiprocessing as mp

import pytest

from fs_structs.fslist_simple import FSListSimple
from fs_structs.structs import FSList, FSUDict, lock_context

N_ITEMS = 50
N_PRODUCERS = 2
N_CONSUMERS = 2
N_INCREMENTS = 50
N_INCREMENTERS = 4

LIST_CLASSES = {"FSList": FSList, "FSListSimple": FSListSimple}


def _producer(root, tag, n, cls_name="FSList"):
    queue = LIST_CLASSES[cls_name](root / "queue", root / "tmp")
    for i in range(n):
        queue.append((tag, i))


def _consumer(root, idx, cls_name="FSList"):
    queue = LIST_CLASSES[cls_name](root / "queue", root / "tmp")
    taken = []
    while True:
        try:
            taken.append(queue.pop_left(timeout=30, watchdog_timeout=1, wait=(0.0, 0.01)))
        except IndexError:
            break
    FSUDict(root / "results", root / "tmp")[idx] = taken


def _incrementer(root, n):
    counter = FSUDict(root / "counter", root / "tmp")
    for _ in range(n):
        with lock_context(root / "counter", "c", timeout=120, watchdog_timeout=2, wait=(0.0, 0.01)):
            counter["n"] = counter.get("n", 0) + 1


def _run(processes, timeout=180):
    for p in processes:
        p.start()
    for p in processes:
        p.join(timeout)
    assert all(p.exitcode == 0 for p in processes), [p.exitcode for p in processes]


@pytest.mark.parametrize("cls_name", list(LIST_CLASSES))
def test_concurrent_consumers_take_each_item_exactly_once(root, cls_name):
    ctx = mp.get_context("spawn")
    _run([ctx.Process(target=_producer, args=(root, tag, N_ITEMS, cls_name)) for tag in "ab"[:N_PRODUCERS]])

    queue = LIST_CLASSES[cls_name](root / "queue", root / "tmp")
    assert len(queue) == N_PRODUCERS * N_ITEMS

    _run([ctx.Process(target=_consumer, args=(root, i, cls_name)) for i in range(N_CONSUMERS)])

    results = FSUDict(root / "results", root / "tmp")
    per_consumer = [results[i] for i in range(N_CONSUMERS)]
    taken = [item for items in per_consumer for item in items]

    expected = [(tag, i) for tag in "ab"[:N_PRODUCERS] for i in range(N_ITEMS)]
    assert sorted(taken) == sorted(expected)  # every item once, none lost, none duplicated
    assert len(queue) == 0
    assert not (queue.base_path / "pop_left_lock.lock").exists()

    # pop_left is FIFO: what each consumer took from one producer is in increasing order.
    for items in per_consumer:
        for tag in "ab"[:N_PRODUCERS]:
            seq = [i for t, i in items if t == tag]
            assert seq == sorted(seq)


def test_lock_free_pop_left_under_contention(root):
    """FSList only: four consumers race on the same elements without any lock.

    On Windows two renames of one file can both succeed (see _take_file); this test would
    show duplicates or crashed consumers if the loser did not give the element up.
    """
    ctx = mp.get_context("spawn")
    n_items, n_consumers = 100, 4
    _run([ctx.Process(target=_producer, args=(root, tag, n_items, "FSList")) for tag in "ab"])

    _run([ctx.Process(target=_consumer, args=(root, i, "FSList")) for i in range(n_consumers)])

    results = FSUDict(root / "results", root / "tmp")
    taken = [item for i in range(n_consumers) for item in results[i]]
    expected = [(tag, i) for tag in "ab" for i in range(n_items)]
    assert sorted(taken) == sorted(expected)  # every item once, none lost, none duplicated
    assert len(FSList(root / "queue", root / "tmp")) == 0
    assert list((root / "tmp").iterdir()) == []  # no temp file left behind


def test_lock_serializes_processes(root):
    ctx = mp.get_context("spawn")
    _run([ctx.Process(target=_incrementer, args=(root, N_INCREMENTS)) for _ in range(N_INCREMENTERS)])

    counter = FSUDict(root / "counter", root / "tmp")
    assert counter["n"] == N_INCREMENTS * N_INCREMENTERS
    assert not (root / "counter" / "c.lock").exists()
