'''
This file include logical base class for maintain B-plus Tree at the
bottom of database.
- ValueRef: referent to a value of node of B+ Tree and provide interface
			of operations which do real manipulations to database.
- BaseTree: basement of an b+ tree.
'''

from abc import abstractmethod


class ValueRef(object):
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
	'''
	Separate database-about operations with B+Tree-about operations.
	_tree_ref / _get / _insert / _delete are from subclass.
	'''
	node_ref_class = None
	value_ref_class = ValueRef

	def __init__(self, storage):
		self._storage = storage
		self._refresh_tree_ref()

	def commit(self):
		self._tree_ref.write_data(self._storage)
		self._storage.commit_root_address(self._tree_ref.address)

	def _refresh_tree_ref(self):
		self._tree_ref = self.node_ref_class(  # refresh the view of all tree.
			address=self._storage.get_root_address())

	def get(self, key):
		if not self._storage.locked:
			self._refresh_tree_ref()
		return self._get(self._follow(self._tree_ref), key)

	def set(self, key, value):
		if not self._storage.locked:
			self._storage.lock()
			self._refresh_tree_ref()
		self._tree_ref = self._insert(
			self._follow(self._tree_ref), key, self.value_ref_class(value))

	def pop(self, key):
		if not self._storage.locked:
			self._storage.lock()
			self._refresh_tree_ref()
		self._tree_ref = self._delete(
			self._follow(self._tree_ref), key)

	def _follow(self, ref):
		return ref.read_data(self._storage)

	def __len__(self):
		if not self._storage.locked:
			self._refresh_tree_ref()
		root = self._follow(self._tree_ref)
		if root:
			return root.length
		else:
			return 0
