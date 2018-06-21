"""
This file include the main interface of CannonDB.
"""
import threading
import time
from abc import ABCMeta

from cannondb.constants import DEFAULT_CHECKPOINT_SECONDS
from cannondb.storages import FileStorage, MemoryStorage
from cannondb.utils import with_metaclass, LRUCache


class CannonDB(with_metaclass(ABCMeta)):
    """
    Main handler for database users, providing clear and easy-to-use interfaces.
    Users can config batch of flexible parameters when create a DB instance, see down below for
    more details.
    """
    DEFAULT_STORAGE = FileStorage

    def __init__(self, file_name: str = 'database', cache_size: int = 256, *, order=100, page_size=8192, key_size=16,
                 value_size=64, file_cache=1024, **kwargs):
        """
        :param file_name: database file name (only used by file storage)
        :param cache_size: maximum key-value pairs contain in cache
        :param order: order setting of underlying B tree (only used by file storage)
        :param page_size: maximum page size(bytes) in each file, file is split into pages. (only used by file storage)
        :param key_size: maximum key size(bytes) limitation (only used by file storage)
        :param value_size: maximum value size(bytes) limitation (only used by file storage)
        :param file_cache: LRU cache size (only used by file storage)
        :param kwargs: specify storage type: 'storage'='file' or 'storage'='memory'
                       multi-process lock open: 'm_process'=True (only used by memory storage)
                       log mode: 'log'='local' (log in local file (log.log))
                                 'log'='tcp' or 'udp': log to concrete host & port
        """
        super().__init__()
        storage = kwargs.pop('storage', None)
        if storage and storage == 'memory':
            self._storage = MemoryStorage(m_process=kwargs.pop('m_process', False))
        else:
            self._storage = FileStorage(file_name, order, page_size, key_size, value_size, file_cache)
        self._cache = LRUCache(capacity=cache_size)
        self._closed = False
        self._checkpoint_th = threading.Thread(target=self._timing_checkpoint,args=(DEFAULT_CHECKPOINT_SECONDS,)).start()

    def insert(self, key, value, override=False):
        """
        :param key: key to be inserted.
        :param value: value to be set corresponding to key.
        :param override: True: allow to override value if key exists, else False.
        """
        self._cache[key] = value
        self._storage.insert(key, value, override)

    def remove(self, key):
        """Remove target key from database file."""
        if key in self._cache:
            del self._cache[key]
        self._storage.remove(key)

    def keys(self):
        """Get all keys stored in database."""
        return self._storage.keys()

    def values(self):
        """Get all values stored in database."""
        return self._storage.values()

    def items(self):
        """Get all key-value pairs stored in database."""
        return self._storage.items()

    def get(self, key, default=None):
        """
        :param key: key expected to be searched in the tree.
        :param default: if key doesn't exist, return default.
        :return: value corresponding to the key if key exists.
        """
        if key in self._cache:
            return self._cache[key]
        return self._storage.get(key, default)

    def _timing_checkpoint(self, secs: int):
        """Do checkpoint every `secs` seconds, used by one specific working thread."""
        while not self._closed:
            time.sleep(secs)
            self.checkpoint()

    def checkpoint(self):
        """Manually perform checkpoint"""
        self._storage.checkpoint()

    def commit(self):
        """Commit all changes and let database do persistence."""
        self._storage.commit()

    def set_auto_commit(self, auto: bool):
        """
        :param auto: True: db will commit when open a transaction every time.
                     False: commit util user manually call .commit(), for boosting performance.
        """
        self._storage.set_auto_commit(auto)

    def close(self):
        """
        Close the database and exit safely.
        """
        if hasattr(self, '_logger'):
            self._logger.close()
        self._storage.close()
        self._closed = True
        self._checkpoint_th.join()

    @property
    def is_open(self):
        return not self._closed

    @property
    def size(self):
        """Number of total key-value pairs."""
        return len(self._storage)

    """
    Magic methods defined to expand flexibility and ease of use.
    """
    def __contains__(self, item):
        return self._storage.__contains__(item)

    def __len__(self):
        return self.size

    def __iter__(self):
        return self._storage.__iter__()

    def __enter__(self):
        return self._storage.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    def __setitem__(self, key, value):
        self.insert(key, value, override=True)

    __getitem__ = get
    __delitem__ = remove
