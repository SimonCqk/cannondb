'''
This file include storage wrappers to expand the functionality of storage.
- StorageWrapper: Abstract base wrapper.
- CacheStorage: Use cache to improve performance.
- LogStorage: Use logs to record all pivotal operations.
'''


class StorageWrapper(object):
	pass


class CacheStorage(StorageWrapper):
	pass


class LogStorage(StorageWrapper):
	pass
