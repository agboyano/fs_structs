import json
import logging
import os
import pickle
import random
import shutil
import time
import uuid
from ast import literal_eval
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import joblib

from .watchdog import wait_until


def timestamp():
    return datetime.now().isoformat()

logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)

def sleep(a, b=None):
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a, b))


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
    with open(filename, "r") as f:
        return json.load(f)


json_serializer = FSSerializer(json_dump, json_load, "json")


def joblib_dump(value, filename):
    with open(filename, "wb") as f:
        joblib.dump(
            value, f
        )  # a joblib.dump le puedo pasar una cadena o un file handler
        f.flush()
        os.fsync(f.fileno())


def joblib_load(filename):
    return joblib.load(filename)


joblib_serializer = FSSerializer(joblib_dump, joblib_load, "jbl")


class FSUDict:
    def __init__(
        self, base_path: str, temp_dir: str, serializer=joblib_serializer, fast=False
    ):
        self.base_path = Path(base_path).resolve()
        self.temp_dir = Path(temp_dir).resolve()

        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.load = serializer.load
        self.dump = serializer.dump
        self.ext = serializer.extension

        self.fast = fast

    def _key_to_filename(self, key):
        return repr(key).encode().hex() + "." + self.ext

    def _filename_to_key(self, filename):
        s = ".".join(filename.split(".")[:-1])
        return literal_eval(bytes.fromhex(s).decode())
        #return literal_eval(".".join(filename.split(".")[:-1]))

    def __setitem__(self, key, value) :
        """Store value using atomic write pattern"""
        target_path = self.base_path / self._key_to_filename(key)

        if self.fast:
            self.dump(value, target_path)

        else:
            temp_path = self.temp_dir / f"tmp_{uuid.uuid4().hex}"
            try:
                self.dump(value, temp_path)

                try:
                    temp_path.rename(target_path)
                except (FileExistsError, PermissionError):
                    target_path.unlink()
                    temp_path.rename(target_path)
            finally:
                if temp_path.exists():
                    temp_path.unlink()

    def __getitem__(self, key):
        target_path = self.base_path / self._key_to_filename(key)
        try:
            return self.load(target_path)
        except FileNotFoundError:
            raise KeyError(key)

    def __delitem__(self, key):
        target_path = self.base_path / self._key_to_filename(key)
        try:
            target_path.unlink()
        except FileNotFoundError:
            raise KeyError(key)

    def __contains__(self, key):
        return (self.base_path / self._key_to_filename(key)).is_file()

    def keys(self):
        return [
            self._filename_to_key(f.name)
            for f in os.scandir(self.base_path)
            if f.is_file()
        ]
        # return [self._filename_to_key(f.name) for f in self.base_path.iterdir() if f.is_file()]

    def values(self):
        return [self[key] for key in self.keys()]

    def items(self):
        return [(key, self[key]) for key in self.keys()]

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return (k for k in self.keys())

    def clear(self):
        shutil.rmtree(self.base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)  # por si tmp en base_path
        """
        for key in self.keys():
            del self[key]
        """

    def update(self, iterable):
        for k, v in iterable:
            self[k] = v

    def get(self, key, default=None):
        if key in self:
            return self[key]
        else:
            return default

    def pop(self, key):
        try:
            if self.fast:
                value = self[key]
                del self[key]
            else:
                temp_path = self.temp_dir / f"tmp_{uuid.uuid4().hex}"
                target_path = self.base_path / self._key_to_filename(key)
                target_path.rename(temp_path)
                value = self.load(temp_path)
                temp_path.unlink()
            return value
        except FileNotFoundError:
            raise KeyError(key)
        

class TimeOrderedTuple():
    """
    From python 3.7 time.time() ±1 µs precision (not true in windows)
    Using time.perf_counter_ns() to improve precision to ns. 
    time.time_ns() not reliable in windows.
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
        for i in range(N-1):

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
    def __init__(self, base_path: str, temp_dir: str, serializer=joblib_serializer, fast=False):
        self.data = FSUDict(base_path, temp_dir, serializer, fast=fast)
        self.base_path = self.data.base_path
        self.serializer = serializer
        self.key_generator = TimeOrderedTuple()
        self.id = uuid.uuid4().hex # v7 is time ordered, not implemented yet
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
            self.data[self._new_mid_key(keys[index-1], keys[index])] = value
        else:
            raise IndexError("list assignment index out of range")

    def values(self):
        return [self.data[k] for k in self.keys()]

    def items(self):
        return [(self.data[k], k) for k in self.keys()]

    def keys(self):
        return sorted(self.data.keys())

    def __delitem__(self, index):
        del self.data[self.data.keys()[index]]

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
            if (len(li) != len(value)) and step !=1:
                raise ValueError(f"attempt to assign sequence of size {len(value)} to extended slice of size {len(li)}")
            
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

    def pop_left(self, timeout=-1.0, watchdog_timeout=19, wait=(0.0, 0.0)):
        """
        timeout: Maximum time to wait in seconds (< 0 = waits indefinitely, 0 = try once)
        Raises IndexError if no data available in list
        Raises LockingError if couldn't get Lock or timeout trying
        """
        if self.fast:
            ix = self.keys()[0]
            v = self.data.pop(ix)
            return v
        else:
            with lock_context(self.data.base_path, "pop_left_lock", timeout, watchdog_timeout, wait):
                ix = self.keys()[0]
                v = self.data.pop(ix)
                return v
        raise IndexError("pop_left: empty list") # It should not happen


class FSNamespace:
    def __init__(self, base_path: str, temp_dir=None, serializer=joblib_serializer):
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

    def udict(self, name, fast=False):
        nt = [x for x in self.names_types() if x[0] == name]
        if len(nt) > 0 and nt[0][1] != self.udict_prefix:
            raise ValueError(f"{name} exits with type {nt[0][1]}")
        return FSUDict(
            self.base_path / (self.udict_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
            fast=fast
        )

    def list(self, name, fast=False):
        nt = [x for x in self.names_types() if x[0] == name]
        if len(nt) > 0 and nt[0][1] != self.list_prefix:
            raise ValueError(f"{name} exits with type {nt[0][1]}")
        return FSList(
            self.base_path / (self.list_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
            fast=fast
        )

    def namespace(self, name):
        nt = [x for x in self.names_types() if x[0] == name]
        if len(nt) > 0 and nt[0][1] != self.list_prefix:
            raise ValueError(f"{name} exits with type {nt[0][1]}")
        return FSNamespace(
            self.base_path / (self.namespace_prefix + self.sep + name), 
            self.temp_dir, 
            self.serializer
        )

    def clear(self, clear_tmp=True):
        for d in [
            d for d in self.base_path.iterdir() if d.is_dir() if d != self.temp_dir
        ]:
            shutil.rmtree(d)
        for f in [f for f in self.base_path.iterdir() if f.is_file()]:
            f.unlink()
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def clear_tmp(self):
        for f in [f for f in self.temp_dir.iterdir() if f.is_file()]:
            f.unlink()

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
                fast
            )

        elif len(nt) > 0 and nt[0][1] == self.list_prefix:
            return FSList(
                self.base_path / (self.list_prefix + self.sep + name),
                self.temp_dir,
                self.serializer,
                fast
            )
        
        elif len(nt) > 0 and nt[0][1] == self.namespace_prefix:
            return FSNamespace(
                self.base_path / (self.namespace_prefix + self.sep + name),
                self.temp_dir,
                self.serializer
            )
        else :
            raise ValueError(f"{name} not available in namespace")
    
    def __getattr__(self, name):
        return self.variable(name)


class LockingError(Exception):
    def __init__(self, message):
        super().__init__(message)


def acquire_lock(base_path, lock_name, timeout=-1.0, watchdog_timeout=19, wait=(0.0, 0.0)):
    """
    timeout: Maximum time to wait in seconds (< 0 = wait indefinitely, 0 = try once)
    """
    dir_name = lock_name + ".lock"
    abs_path = Path(base_path) / dir_name

    def try_lock():
        try:
            # Atomically create a directory (mkdir is atomic on most FS)
            os.mkdir(abs_path)
            return True
        except (FileExistsError, PermissionError):
            return False

    if try_lock():
        logger.debug(f"{timestamp()} {lock_name} set in {base_path}")
        return True

    start_time = time.time()
    time_left = timeout
    wait_forever = True if timeout < -0.001 else False

    while time_left > 0.0 or wait_forever:
        new_watchdog_timeout = watchdog_timeout if wait_forever else min(watchdog_timeout, time_left)

        _ = wait_until(
            [base_path],
            lambda x: x.is_directory
            and x.event_type == "deleted"
            and dir_name == Path(x.src_path).resolve().name,
            timeout=new_watchdog_timeout,
        )
        sleep(*wait)

        if try_lock():
            logger.debug(f"{timestamp()} {lock_name} set in {base_path}")
            return True
        
        time_left = timeout - (time.time() - start_time)

    raise LockingError(f"{timestamp()} Couldn`t get lock {lock_name} in {base_path}")


def release_lock(base_path, lock_name):
    os.rmdir(Path(base_path) / (lock_name + ".lock"))
    logger.debug(f"{timestamp()} {lock_name} unset in {base_path}")


@contextmanager
def lock_context(base_path, lock_name, timeout=60, watchdog_timeout=19, wait=(0.0, 0.0)):
    acquire_lock(base_path, lock_name, timeout, watchdog_timeout, wait)
    try:
        yield  # execution resumes here when the context is entered
    finally:
        release_lock(base_path, lock_name)