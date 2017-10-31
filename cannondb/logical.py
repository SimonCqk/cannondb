'''
This file include logical base class for maintain B-plus Tree at the
bottom of database.
- ValueRef: referent to a value of node of B+ Tree and provide interface
			of operations which do real manipulations to database.
- BaseTree: basement of an b+ tree.
'''

from abc import abstractmethod


class NodeRef(object):
	def __init__(self, referent=None, address=0):
		assert 0 <= address < 0xFFFFFFFFFFFFFFFF
		self._referent = referent
		self._address = address

	@abstractmethod
	def prepare_to_store(self, storage):
		pass

	@staticmethod
	def referent_to_string(referent):
		return referent.encode('utf-8')

	@staticmethod
	def string_to_referent(string):
		return string.decode('utf-8')

	def read_data(self, storage):
		if self._referent is None and self._address:
			self._referent = self.string_to_referent(
				storage.read(self._address))
		return self._referent

	def write_data(self, storage):
		if self._referent is not None:
			self.prepare_to_store(storage)
			self._address = \
				storage.write(self.referent_to_string(self._referent), self._address)

	@property
	def address(self):
		return self._address

	@address.setter
	def address(self, value):
		raise RuntimeError("Can't set address over referent.")


class BaseTree(object):
	pass
