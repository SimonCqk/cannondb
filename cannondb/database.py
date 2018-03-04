"""
This file include the main interface of CannonDB.
"""
from abc import ABCMeta

from cannondb.storages import FileStorage, MemoryStorage
from cannondb.utils import with_metaclass, LRUCache


class CannonDB(with_metaclass(ABCMeta)):
    """   """
    DEFAULT_STORAGE = FileStorage

    def __init__(self, file_name: str = 'database', cache_size: int = 256, *, order=100, page_size=8192, key_size=16,
                 value_size=64, file_cache=1024, **kwargs):
        """
        :param file_name: database file name (only used by file storage)
        :param cache_size: cache
        :param order: order setting of underlying B tree (only used by file storage)
        :param page_size: maximum page size in each file, file is split into pages. (only used by file storage)
        :param key_size: maximum _key size limitation (only used by file storage)
        :param value_size: maximum _value size limitation (only used by file storage)
        :param file_cache: LRU cache size (only used by file storage)
        :param kwargs: to specify storage type: storage='file' or storage='memory'
                                  multi-process lock open: m_process=True (only used by memory storage)
        """
        super().__init__()
        storage = kwargs.pop('storage', None)
        if storage and storage == 'memory':
            self._storage = MemoryStorage(m_process=kwargs.pop('m_process', False))
        else:
            self._storage = FileStorage(file_name, order, page_size, key_size, value_size, file_cache)
        self._cache = LRUCache(capacity=cache_size)
        self._closed = False

    def insert(self, key, value, override=False):
        self._cache[key] = value
        self._storage.insert(key, value, override)

    def remove(self, key):
        if key in self._cache:
            del self._cache[key]
        self._storage.remove(key)

    def keys(self):
        return self._storage.keys()

    def values(self):
        return self._storage.values()

    def items(self):
        return self._storage.items()

    def get(self, key, default=None):
        if key in self._cache:
            return self._cache[key]
        return self._storage.get(key, default)

    def close(self):
        self._storage.close()
        self._closed = True

    @property
    def is_open(self):
        return not self._closed

    @property
    def size(self):
        return len([_ for _ in self._storage])

    def __contains__(self, item):
        return self._storage.__contains__(item)

    def __len__(self):
        return self.size

    def __iter__(self):
        return self._storage.__iter__()

    def __enter__(self):
        return self._storage.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_open:
            self._closed = True
        return self._storage.__exit__(exc_type, exc_val, exc_tb)

    def __setitem__(self, key, value):
        self.insert(key, value, override=True)

    __getitem__ = get
    __delitem__ = remove
