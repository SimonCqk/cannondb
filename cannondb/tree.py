"""
This file include the concrete implementation of B+ tree.

"""
import bisect
import math
import operator
from abc import abstractmethod, ABCMeta
from typing import Iterable


class _Node(metaclass=ABCMeta):
    """Abstract base class of b plus node"""
    __slots__ = ()

    @abstractmethod
    def split(self):
        pass

    @abstractmethod
    def grow(self, ancestors):
        pass

    @abstractmethod
    def shrink(self, ancestors):
        pass

    @abstractmethod
    def lateral(self, parent, parent_index, dest, dest_index):
        pass

    @classmethod
    def from_node(cls, node, **kwargs):
        if isinstance(node, _BPlusLeaf):
            return cls(tree=kwargs.get('tree', node.tree),
                       keys=kwargs.get('keys', node.keys),
                       values=kwargs.get('values', node.values),
                       next=kwargs.get('next', node.next))
        elif isinstance(node, _BPlusBranch):
            return cls(tree=kwargs.get('tree', node.tree),
                       keys=kwargs.get('keys', node.keys),
                       children=kwargs.get('children', node.children))
        else:
            raise TypeError('Invalid node type passed in.')

    def __repr__(self):
        name = 'Branch' if hasattr(self, 'children') else 'Leaf'
        return '<{name} [{contents}]>'.format(
            name=name, contents=', '.join([str(i) for i in self.keys]))


class _BPlusBranch(_Node):
    __slots__ = ("tree", "keys", "children")

    def __init__(self, tree, keys=None, children=None):
        self.tree = tree
        self.keys = keys or list()
        self.children = children or list()
        if self.children:
            assert len(self.keys) + 1 == len(self.children), \
                "One more child than values item required."

    def split(self):
        center = len(self.keys) // 2
        median = self.keys[center]
        sibling = type(self)(tree=self.tree,
                             keys=self.keys[center + 1:],
                             children=self.children[center + 1:])
        self.keys = self.keys[:center]
        self.children = self.children[:center + 1]
        return sibling, median

    def grow(self, ancestors=None):
        parent, parent_index = ancestors.pop()

        minimum = math.ceil(self.tree._order / 2)
        left_sib = right_sib = None

        # try to borrow from the right sibling
        if parent_index + 1 < len(parent.children):
            right_sib = parent.children[parent_index + 1]
            if len(right_sib.keys) >= minimum:
                right_sib.lateral(parent, parent_index + 1, self, parent_index)
                return

        # try to borrow from the left sibling
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len(left_sib.keys) >= minimum:
                left_sib.lateral(parent, parent_index - 1, self, parent_index)
                return

        # consolidate with a sibling - try left first
        if left_sib:
            left_sib.keys.append(parent.keys[parent_index - 1])
            left_sib.keys.extend(self.keys)
            if self.children:
                left_sib.children.extend(self.children)
            parent.keys.pop(parent_index - 1)
            parent.children.pop(parent_index)
        else:
            self.keys.append(parent.keys[parent_index])
            self.keys.extend(right_sib.keys)
            if self.children:
                self.children.extend(right_sib.children)
            parent.keys.pop(parent_index)
            parent.children.pop(parent_index + 1)

        if len(parent.keys) < minimum:
            if ancestors:
                # parent is not the root
                parent.grow(ancestors)
            elif not parent.keys:
                # parent is root, and its now empty
                self.tree._root = left_sib or self

    def shrink(self, ancestors=None):
        parent = None

        if ancestors:
            parent, parent_index = ancestors.pop()
            # try to lend to the left neighboring sibling
            if parent_index:
                left_sib = parent.children[parent_index - 1]
                if len(left_sib.keys) < self.tree._order:
                    self.lateral(
                        parent, parent_index, left_sib, parent_index - 1)
                    return

            # try the right neighbor
            if parent_index + 1 < len(parent.children):
                right_sib = parent.children[parent_index + 1]
                if len(right_sib.keys) < self.tree._order:
                    self.lateral(
                        parent, parent_index, right_sib, parent_index + 1)
                    return

        sibling, median = self.split()

        if not parent:  # this is root node
            parent, parent_index = self.tree.BRANCH(
                tree=self.tree, children=[self]), 0
            self.tree._root = parent
        # pass the median up to the parent
        parent.keys.insert(parent_index, median)
        parent.children.insert(parent_index + 1, sibling)
        if len(parent.keys) > parent.tree._order:
            parent.shrink(ancestors)

    def lateral(self, parent, parent_index, dest, dest_index):
        if parent_index > dest_index:  # lend to the left neighboring sibling
            dest.keys.append(parent.keys[dest_index])
            parent.keys[dest_index] = self.keys.pop(0)
            if self.children:
                dest.children.append(self.children.pop(0))
        else:  # lend to the right neighboring sibling
            dest.keys.insert(0, parent.keys[parent_index])
            parent.keys[parent_index] = self.keys.pop()
            if self.children:
                dest.children.insert(0, self.children.pop())

    def insert(self, index, item, ancestors=None):
        self.keys.insert(index, item)
        if len(self.keys) > self.tree._order:
            self.shrink(ancestors)

    def remove(self, index, ancestors=None):
        minimum = math.ceil(self.tree._order / 2)

        if self.children:
            # try promoting from the right subtree first,
            # but only if it won't have to resize
            additional_ancestors = [(self, index + 1)]
            descendant = self.children[index + 1]
            while hasattr(descendant, 'children'):
                additional_ancestors.append((descendant, 0))
                descendant = descendant.children[0]
            if len(descendant.keys) >= minimum:
                ancestors.extend(additional_ancestors)
                self.keys[index] = descendant.keys[0]
                descendant.remove(0, ancestors)
                return
            # fall back to the left child
            additional_ancestors = [(self, index)]
            descendant = self.children[index]
            while hasattr(descendant, 'children'):
                additional_ancestors.append(
                    (descendant, len(descendant.children) - 1))
                descendant = descendant.children[-1]
            assert len(descendant.keys) >= minimum
            ancestors.extend(additional_ancestors)
            self.keys[index] = descendant.keys[-1]
            descendant.remove(len(descendant.children) - 1, ancestors)
        else:
            self.keys.pop(index)
            if len(self.keys) < minimum and ancestors:
                self.grow(ancestors)


class _BPlusLeaf(_Node):
    __slots__ = ("tree", "keys", "values", "next")

    def __init__(self, tree, keys=None, values=None, next=None):
        self.tree = tree
        self.keys = keys or []
        self.values = values or []
        self.next = next  # point to the sibling
        assert len(self.keys) == len(self.values), "one values per key"

    def shrink(self, ancestors=None):
        parent = None

        if ancestors:
            parent, parent_index = ancestors.pop()
            # try to lend to the left neighboring sibling
            if parent_index:
                left_sib = parent.children[parent_index - 1]
                if len(left_sib.keys) < self.tree._order:
                    self.lateral(
                        parent, parent_index, left_sib, parent_index - 1)
                    return

            # try the right neighbor
            if parent_index + 1 < len(parent.children):
                right_sib = parent.children[parent_index + 1]
                if len(right_sib.keys) < self.tree._order:
                    self.lateral(
                        parent, parent_index, right_sib, parent_index + 1)
                    return
        # not returned.
        # which means self has to split and then rebuild.
        sibling, push = self.split()

        if not parent:
            parent, parent_index = self.tree.BRANCH(
                tree=self.tree, children=[self]), 0
            self.tree._root = parent

        # pass the median up to the parent
        parent.keys.insert(parent_index, push)
        parent.children.insert(parent_index + 1, sibling)
        if len(parent.keys) > parent.tree._order:
            parent.shrink(ancestors)

    def lateral(self, parent, parent_index, dest, dest_index):
        if parent_index > dest_index:  # lend to the left neighboring sibling
            dest.keys.append(self.keys.pop(0))
            dest.values.append(self.values.pop(0))
            parent.keys[dest_index] = self.keys[0]
        else:  # lend to the right
            dest.keys.insert(0, self.keys.pop())
            dest.values.insert(0, self.values.pop())
            parent.keys[parent_index] = dest.keys[0]

    def split(self):
        center = len(self.keys) // 2
        sibling = type(self)(tree=self.tree,
                             keys=self.keys[center:],
                             values=self.values[center:],
                             next=self.next)
        self.keys = self.keys[:center]
        self.values = self.values[:center]
        self.next = sibling
        return sibling, sibling.keys[0]

    def grow(self, ancestors):
        minimum = math.ceil(self.tree._order / 2)
        parent, parent_index = ancestors.pop()
        left_sib = right_sib = None

        # try borrowing from a neighbor - try right first
        if parent_index + 1 < len(parent.children):
            right_sib = parent.children[parent_index + 1]
            if len(right_sib.keys) >= minimum:
                right_sib.lateral(parent, parent_index + 1, self, parent_index)
                return

        # fallback to left
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len(left_sib.keys) >= minimum:
                left_sib.lateral(parent, parent_index - 1, self, parent_index)
                return

        # join with a neighbor - try left first
        if left_sib:
            left_sib.keys.extend(self.keys)
            left_sib.values.extend(self.values)
            parent.remove(parent_index - 1, ancestors)
            return

        # fallback to right
        self.keys.extend(right_sib.keys)
        self.values.extend(right_sib.values)
        parent.remove(parent_index, ancestors)

    def remove(self, index, ancestors):
        minimum = math.ceil(self.tree._order / 2)
        if index >= len(self.keys):
            self, index = self.next, 0

        key = self.keys[index]

        # if any leaf that could accept the key can do so
        # without any rebalancing necessity, then go that route
        current = self
        while current is not None and current.keys[index] == key:
            if len(current.keys) >= minimum or not ancestors:
                if current.keys[0] == key:
                    index = 0
                else:
                    index = bisect.bisect_left(current.keys, key)
                current.keys.pop(index)
                current.values.pop(index)
                return
            current = current.next
        self.grow(ancestors)

    def insert(self, index, key, value, ancestors=None):
        self.keys.insert(index, key)
        self.values.insert(index, value)

        if len(self.keys) > self.tree._order:
            self.shrink(ancestors)


class BPlusTree(object):
    LEAF = _BPlusLeaf
    BRANCH = _BPlusBranch

    def __init__(self, order=100):
        self._order = order
        self._root = self._bottom = self.LEAF(self)

    def _get(self, key):
        """
        :return: return nothing if key is not in tree,
                 else yield all required items.
        """
        node, index = self._path_to(key)[-1]

        if index == len(node.keys):
            if node.next:
                node, index = node.next, 0
            else:
                return

        while node.keys[index] == key:
            yield node.values[index]
            index += 1
            if index == len(node.keys):
                if node.next:
                    node, index = node.next, 0
                else:
                    return

    def _path_to_branch(self, key):
        """
        :return: ancestors:list from root to key node (usually branch node)
        """
        current = self._root
        ancestry = []
        while hasattr(current, "children"):
            index = bisect.bisect_left(current.keys, key)
            ancestry.append((current, index))
            if index < len(current.keys) \
                    and current.keys[index] == key:
                return ancestry
            current = current.children[index]

        index = bisect.bisect_left(current.keys, key)
        ancestry.append((current, index))

        return ancestry

    def _path_to(self, key):
        """
        :return: the complete path from root to key node (leaf node)
        """
        path = self._path_to_branch(key)
        node, index = path[-1]
        while hasattr(node, "children"):
            node = node.children[index]
            index = bisect.bisect_left(node.keys, key)
            path.append((node, index))
        return path

    def get(self, key, default=None):
        try:
            return next(self._get(key))
        except StopIteration:
            return default

    def getlist(self, key):
        return list(self._get(key))

    def insert(self, key, value, override=False):
        """
        Insert a pair of key-value into tree
        :param override: if key has existed and override is True,
                         then override the origin value.
        """
        if key in self:
            if override:
                path = self._path_to(key)
                node, index = path.pop()
                node.values[index] = value
            else:
                raise KeyError('key \'{key}\' has existed'.format(key=key))
        else:
            path = self._path_to(key)
            node, index = path.pop()
            node.insert(index, key, value, path)

    def remove(self, key):
        """
        remove a exist key and its value, raise an exception if key doesn't exist
        """
        if key in self:
            path = self._path_to(key)
            node, index = path.pop()
            node.remove(index, path)
        else:
            raise KeyError('key \'{key}\' does not exist'.format(key=key))

    __getitem__ = get
    __setitem__ = insert
    __delitem__ = remove

    def __contains__(self, key):
        for _ in self._get(key):
            return True
        return False

    def __repr__(self):
        def recurse(node, accum, depth):
            accum.append(("  " * depth) + repr(node))
            for node in getattr(node, 'children', []):
                recurse(node, accum, depth + 1)

        accum = []
        recurse(self._root, accum, 0)
        return '\n'.join(accum)

    def iteritems(self):
        node = self._root
        while hasattr(node, "children"):
            node = node.children[0]

        while node:
            for pair in zip(node.keys, node.values):
                yield pair
            node = node.next

    def iterkeys(self):
        return [operator.itemgetter(0)(i) for i in self.iteritems()]

    def itervalues(self):
        return [operator.itemgetter(1)(i) for i in self.iteritems()]

    __iter__ = iterkeys

    def items(self):
        return list(self.iteritems())

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def multi_insert(self, pairs: Iterable, override=False):
        """
        Insert multiple key-value pairs at one time.
        :param pairs: iter obj which contains a batch of key-value pairs.
        :param override: If True, new value will override old values which have existed.
        """
        if not isinstance(pairs, Iterable):
            raise TypeError('pairs should be a iterable object')
        elif isinstance(pairs, (tuple, list, set)):
            sorted(pairs, key=lambda it: it[0])  # sort by key
            for key, value in pairs:
                self.insert(key, value, override)
        elif isinstance(pairs, dict):
            for key, value in pairs.items():
                self.insert(key, value, override)

    @property
    def order(self):
        return self._order

    @order.setter
    def order(self, value):
        raise RuntimeError('Order of B+ tree can not be re-assigned')
