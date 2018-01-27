"""
This file include the main interface of cannondb.
"""
from .storages import FileStorage


class CannonDB(object):
    '''

    '''
    DEFAULT_TABLE = '_default'
    DEFAULT_STORAGE = FileStorage

    def __init__(self, file):
        super().__init__()
        self._file = file
