'''
This file include the specific implementation of B+ tree.

'''
import bisect

import itertools
import operator

from .logical import ValueRef, BaseTree


class _BPlusNode(object):
	__slots__ = ['tree', 'contents', 'data', 'next']

	def __init__(self, tree, contents=None, data=None, next=None):
		self.tree = tree
		self.contents = contents or []
		self.data = data or []
		self.next = next
		assert len(self.contents) == len(self.data), "one data per key"

	def __repr__(self):
		name = getattr(self, "children", 0) and "Branch" or "Leaf"
		return "<%s %s>" % (name, ", ".join(map(str, self.contents)))

	def shrink(self, ancestors):
		parent = None
		if ancestors:
			parent, parent_index = ancestors.pop()
			# try to lend to the left neighboring sibling
			if parent_index:
				left_sib = parent.children[parent_index - 1]
				if len(left_sib.contents) < self.tree.order:
					self.lateral(
						parent, parent_index, left_sib, parent_index - 1)
					return
			# try the right neighbor
			if parent_index + 1 < len(parent.children):
				right_sib = parent.children[parent_index + 1]
				if len(right_sib.contents) < self.tree.order:
					self.lateral(
						parent, parent_index, right_sib, parent_index + 1)
					return
		center = len(self.contents) // 2
		sibling, push = self.split()
		if not parent:
			parent, parent_index = self.tree.BRANCH(
				self.tree, children=[self]), 0
			self.tree._root = parent
		# pass the median up to the parent
		parent.contents.insert(parent_index, push)
		parent.children.insert(parent_index + 1, sibling)
		if len(parent.contents) > parent.tree.order:
			parent.shrink(ancestors)

	def insert(self, index, key, data, ancestors):
		self.contents.insert(index, key)
		self.data.insert(index, data)
		if len(self.contents) > self.tree.order:
			self.shrink(ancestors)

	def lateral(self, parent, parent_index, dest, dest_index):
		if parent_index > dest_index:
			dest.contents.append(self.contents.pop(0))
			dest.data.append(self.data.pop(0))
			parent.contents[dest_index] = self.contents[0]
		else:
			dest.contents.insert(0, self.contents.pop())
			dest.data.insert(0, self.data.pop())
			parent.contents[parent_index] = dest.contents[0]

	def split(self):
		center = len(self.contents) // 2
		median = self.contents[center - 1]
		sibling = type(self)(
			self.tree,
			self.contents[center:],
			self.data[center:],
			self.next)
		self.contents = self.contents[:center]
		self.data = self.data[:center]
		self.next = sibling
		return sibling, sibling.contents[0]

	def remove(self, index, ancestors):
		minimum = self.tree.order // 2
		if index >= len(self.contents):
			self, index = self.next, 0
		key = self.contents[index]
		# if any leaf that could accept the key can do so
		# without any rebalancing necessary, then go that route
		current = self
		while current is not None and current.contents[0] == key:
			if len(current.contents) > minimum:
				if current.contents[0] == key:
					index = 0
				else:
					index = bisect.bisect_left(current.contents, key)
				current.contents.pop(index)
				current.data.pop(index)
				return
			current = current.next
		self.grow(ancestors)

	def grow(self, ancestors):
		minimum = self.tree.order // 2
		parent, parent_index = ancestors.pop()
		left_sib = right_sib = None
		# try borrowing from a neighbor - try right first
		if parent_index + 1 < len(parent.children):
			right_sib = parent.children[parent_index + 1]
			if len(right_sib.contents) > minimum:
				right_sib.lateral(parent, parent_index + 1, self, parent_index)
				return
		# fallback to left
		if parent_index:
			left_sib = parent.children[parent_index - 1]
			if len(left_sib.contents) > minimum:
				left_sib.lateral(parent, parent_index - 1, self, parent_index)
				return
		# join with a neighbor - try left first
		if left_sib:
			left_sib.contents.extend(self.contents)
			left_sib.data.extend(self.data)
			parent.remove(parent_index - 1, ancestors)
			return
		# fallback to right
		self.contents.extend(right_sib.contents)
		self.data.extend(right_sib.data)
		parent.remove(parent_index, ancestors)


class BPlusNodeRef(ValueRef):
	def prepare_to_store(self, storage):
		pass

	@staticmethod
	def referent_to_string(referent):
		pass

	@staticmethod
	def string_to_referent(string):
		pass


class BPlusTree(BaseTree):
	LEAF = _BPlusNode

	def __init__(self, order):
		self.order = order
		self._root = self._bottom = self.LEAF(self)

	def __repr__(self):
		def recurse(node, accum, depth):
			accum.append(("  " * depth) + repr(node))
			for node in getattr(node, "children", []):
				recurse(node, accum, depth + 1)

		accum = []
		recurse(self._root, accum, 0)
		return "\n".join(accum)

	@classmethod
	def bulkload(cls, items, order):
		tree = object.__new__(cls)
		tree.order = order
		leaves = tree._build_bulkloaded_leaves(items)
		tree._build_bulkloaded_branches(leaves)
		return tree

	def _build_bulkloaded_branches(self, leaves, seps):
		minimum = self.order // 2
		levels = [leaves]
		while len(seps) > self.order + 1:
			items, nodes, seps = seps, [[]], []
			for item in items:
				if len(nodes[-1]) < self.order:
					nodes[-1].append(item)
				else:
					seps.append(item)
					nodes.append([])
			if len(nodes[-1]) < minimum and seps:
				last_two = nodes[-2] + [seps.pop()] + nodes[-1]
				nodes[-2] = last_two[:minimum]
				nodes[-1] = last_two[minimum + 1:]
				seps.append(last_two[minimum])
			offset = 0
			for i, node in enumerate(nodes):
				children = levels[-1][offset:offset + len(node) + 1]
				nodes[i] = self.BRANCH(self, contents=node, children=children)
				offset += len(node) + 1
			levels.append(nodes)
		self._root = self.BRANCH(self, contents=seps, children=levels[-1])

	def _get(self, key):
		node, index = self._path_to(key)[-1]
		if index == len(node.contents):
			if node.next:
				node, index = node.next, 0
			else:
				return
		while node.contents[index] == key:
			yield node.data[index]
			index += 1
			if index == len(node.contents):
				if node.next:
					node, index = node.next, 0
				else:
					return

	def _path_to(self, item):
		path = super(BPlusTree, self)._path_to(item)
		node, index = path[-1]
		while hasattr(node, "children"):
			node = node.children[index]
			index = bisect.bisect_left(node.contents, item)
			path.append((node, index))
		return path

	def get(self, key, default=None):
		try:
			return self._get(key).next()
		except StopIteration:
			return default

	def getlist(self, key):
		return list(self._get(key))

	def insert(self, key, data):
		path = self._path_to(key)
		node, index = path.pop()
		node.insert(index, key, data, path)

	def remove(self, key):
		path = self._path_to(key)
		node, index = path.pop()
		node.remove(index, path)

	__getitem__ = get
	__setitem__ = insert
	__delitem__ = remove

	def __contains__(self, key):
		for item in self._get(key):
			return True
		return False

	def iteritems(self):
		node = self._root
		while hasattr(node, "children"):
			node = node.children[0]
		while node:
			for pair in itertools.izip(node.contents, node.data):
				yield pair
			node = node.next

	def iterkeys(self):
		return itertools.imap(operator.itemgetter(0), self.iteritems())

	def itervalues(self):
		return itertools.imap(operator.itemgetter(1), self.iteritems())

	__iter__ = iterkeys

	def items(self):
		return list(self.iteritems())

	def keys(self):
		return list(self.iterkeys())

	def values(self):
		return list(self.itervalues())

	def _build_bulkloaded_leaves(self, items):
		minimum = self.order // 2
		leaves, seps = [[]], []
		for item in items:
			if len(leaves[-1]) >= self.order:
				seps.append(item)
				leaves.append([])
			leaves[-1].append(item)
		if len(leaves[-1]) < minimum and seps:
			last_two = leaves[-2] + leaves[-1]
			leaves[-2] = last_two[:minimum]
			leaves[-1] = last_two[minimum:]
			seps.append(last_two[minimum])
		leaves = [self.LEAF(
			self,
			contents=[p[0] for p in pairs],
			data=[p[1] for p in pairs])
			for pairs in leaves]
		for i in range(len(leaves) - 1):
			leaves[i].next = leaves[i + 1]
		return leaves, [s[0] for s in seps]
