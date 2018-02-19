"""
This file include the main interface of cannondb.
"""
from abc import ABCMeta

from cannondb.storages import FileStorage
from cannondb.utils import with_metaclass


class CannonDB(with_metaclass(ABCMeta)):
    """   """
    DEFAULT_STORAGE = FileStorage

    def __init__(self, file):
        super().__init__()
        self._file = file
