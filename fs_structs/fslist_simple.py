"""``FSListSimple``: the previous ``FSList``, built on top of ``FSUDict``.

``fs_structs.structs.FSList`` (since 0.0.5) stores its own files and is several times
faster. ``FSListSimple`` is the reference implementation: same behaviour, same outputs, but
its file names are different, so the two classes do not see each other's elements. Use it
to read or drain a list written by fs_structs 0.0.4 or earlier.

Examples:
    >>> import tempfile
    >>> from fs_structs.fslist_simple import FSListSimple
    >>> root = tempfile.mkdtemp()
    >>> queue = FSListSimple(root + "/queue", root + "/tmp")
    >>> queue.append({"id": 1})
    >>> queue.pop_left()
    {'id': 1}
"""

import uuid

from .structs import FSUDict, TimeOrderedTuple, joblib_serializer, lock_context


# Implementation notes:
# - This is the FSList of fs_structs <= 0.0.4, moved here unchanged and renamed. The new
#   FSList in structs.py has the same public API and behaviour but its own file names, no
#   dependency on FSUDict and a lock-free pop_left. Keep both in sync when the API changes.
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
class FSListSimple:
    """List stored in a directory, built on top of ``FSUDict``. The previous ``FSList``.

    ``FSList`` is the fast implementation and the one to use. ``FSListSimple`` behaves the
    same and returns the same outputs, but names its files differently: the two classes do
    not see each other's elements. It is kept to read or drain lists written by fs_structs
    0.0.4 or earlier, and as the reference for the behaviour of ``FSList``.

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
        serializer (Serializer): How elements are written and read. Default: joblib.
        fast (bool): If True, elements are written without the atomic rename and without
            fsync, and ``pop_left`` takes no lock. Much faster, but not safe for
            distributed processes nor against a crash. Default False.
        clean (bool): If True, the list is emptied (``clear()``) before it is returned.
            Default False. ``clean=True`` is not atomic (see ``clear()``).

    See Also:
        fs_structs.structs.FSList: The fast implementation with the same behaviour.
        FSUDict: The storage under the list.
        TimeOrderedTuple: Generates the keys that keep the order.

    Examples:
        As a list:

        >>> import tempfile
        >>> from fs_structs.fslist_simple import FSListSimple
        >>> root = tempfile.mkdtemp()
        >>> lst = FSListSimple(root + "/lst", root + "/tmp")
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

        >>> queue = FSListSimple(root + "/queue", root + "/tmp")
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
        >>> fresh = FSListSimple(root + "/queue", root + "/tmp", clean=True)
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
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> lst = FSListSimple(root + "/lst", root + "/tmp")
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
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> lst = FSListSimple(root + "/lst", root + "/tmp")
            >>> lst.extend(range(3))
            >>> lst.values()
            [0, 1, 2]
        """
        for x in iterable:
            self.append(x)

    def copy(self):
        """Return the elements as a plain Python list (not a new ``FSListSimple``).

        Returns:
            list: The elements in order.

        Examples:
            >>> import tempfile
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> lst = FSListSimple(root + "/lst", root + "/tmp")
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
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> lst = FSListSimple(root + "/lst", root + "/tmp")
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
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> lst = FSListSimple(root + "/lst", root + "/tmp")
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
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> lst = FSListSimple(root + "/lst", root + "/tmp")
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
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> lst = FSListSimple(root + "/lst", root + "/tmp")
            >>> lst.extend(["x", "y"])
            >>> keys = lst.keys()
            >>> len(keys) == 2 and keys[0] < keys[1]
            True
        """
        return sorted(self.data.keys())

    def __delitem__(self, index):
        """Remove the element at ``index`` (``del lst[index]``).

        The element at ``index`` is chosen when the call starts, and that exact element is
        deleted even if other processes append or pop meanwhile. ``KeyError`` if another
        process removed it first, ``IndexError`` if the list is shorter than ``index``. No
        lock is taken (the delete is atomic). In a shared queue use ``pop_left`` to consume;
        ``del`` is for removing one specific element you have just looked at.

        See Also:
            FSListSimple: Examples on the class.
        """
        del self.data[self.keys()[index]]

    def clear(self):
        """Remove every element.

        Not safe with other processes: stop producers and consumers first. Clearing a
        queue while consumers run has no defined result.

        Examples:
            >>> import tempfile
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> lst = FSListSimple(root + "/lst", root + "/tmp")
            >>> lst.extend([1, 2, 3])
            >>> lst.clear()
            >>> len(lst)
            0
        """
        self.data.clear()

    def __getitem__(self, index):
        """Return one element (``lst[i]``) or a list of elements (``lst[a:b]``).

        See Also:
            FSListSimple: Examples on the class.
        """
        if isinstance(index, slice):
            return [self.data[k] for k in self.keys()[index]]
        return self.data[self.keys()[index]]

    def __setitem__(self, index, value):
        """Replace one element (``lst[i] = v``) or a slice (``lst[a:b] = values``).

        With a slice and more values than positions, the surplus values are appended at
        the end (a Python list would insert them).

        See Also:
            FSListSimple: Examples on the class.
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
            FSListSimple: Examples on the class.
        """
        return key in [x for x in self]

    def __iter__(self):
        """Iterate over the elements in order (``for x in lst``).

        See Also:
            FSListSimple: Examples on the class.
        """
        return (self.data[k] for k in self.keys())

    def __len__(self):
        """Return the number of elements (``len(lst)``).

        See Also:
            FSListSimple: Examples on the class.
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
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> lst = FSListSimple(root + "/lst", root + "/tmp")
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
            fs_structs.structs.acquire_lock: Meaning of ``timeout``, ``watchdog_timeout``,
                ``wait`` and ``max_age``.
            pop: Removal without a lock, for a single consumer.

        Examples:
            >>> import tempfile
            >>> from fs_structs.fslist_simple import FSListSimple
            >>> root = tempfile.mkdtemp()
            >>> queue = FSListSimple(root + "/queue", root + "/tmp")
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
