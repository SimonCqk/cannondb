'''
This file include the main interface of cannondb.
'''
from .storages import FileStorage, MemoryStorage


class CannonDB(object):
	'''

	'''
	DEFAULT_TABLE = '_default'
	DEFAULT_STORAGE = FileStorage

	def __init__(self, file, *, use_cache=False, use_log=False):
		super().__init__()
