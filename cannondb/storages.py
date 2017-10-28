'''
This file include:
- Storage: Abstract base class for all storage implementation.
- FileStorage: Store data in disk.
- MemoryStorage: Store data in memory.
'''

from abc import ABC, abstractmethod
import os
import struct
import portalocker

class Storage(ABC):
	'''
	Abstract base class for all storage implementation.
	Subclasses must override all these methods.
	'''

	@abstractmethod
	def read(self):
		raise NotImplementedError("Please override this method.")

	@abstractmethod
	def write(self):
		raise NotImplementedError("Please override this method.")

	@abstractmethod
	def close(self):
		raise NotImplementedError("Please override this method.")

	# lock() & unlock ensure the data safety under multi-readers/writers scenarios.
	def lock(self):
		pass

	def unlock(self):
		pass


class FileStorage(Storage):
	pass

class MemoryStorage(Storage):
	pass