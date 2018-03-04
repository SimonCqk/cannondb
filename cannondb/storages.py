"""
- Storage: Abstract base class for all storage implementation.
- FileStorage: Store data into disk.
- MemoryStorage: Store data into memory.
"""

import multiprocessing
import threading

from cannondb.btree import BTree

FileStorage = BTree


class MemoryStorage(object):
    """
    Store _key-_value pairs just in memory.
    """
    __slots__ = ('_memory', '_lock')

    def __init__(self, *, m_process=False):
        super(MemoryStorage, self).__init__()
        self._memory = dict()
        self._lock = threading.Lock() if not m_process else multiprocessing.Lock()

    def insert(self, key, value, override=False):
        with self._lock:
            if key not in self._memory or override:
                self._memory[key] = value
            else:
                raise ValueError('{key} has existed'.format(key=key))

    def remove(self, key):
        with self._lock:
            if key in self._memory:
                self._memory.pop(key)
            else:
                raise KeyError('{key} not in {self}'.format(key=key, self=self.__class__.__name__))

    def get(self, key, default=None):
        return self._memory.get(key, default=default)

    def keys(self):
        return self._memory.keys()

    def values(self):
        return self._memory.values()

    def items(self):
        return self._memory.items()

    def checkpoint(self):
        pass

    def commit(self):
        pass

    def set_auto_commit(self, auto: bool):
        pass

    def close(self):
        self._memory.clear()
