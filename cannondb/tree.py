"""
This file include the concrete implementation of B+ tree.

"""
import bisect
import math
import operator
from abc import abstractmethod
from collections import namedtuple

KeyValPair = namedtuple('KeyValPair', ['key', 'value'])


class _Node(object):
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
        if 'data' in kwargs:
            return cls(tree=kwargs.get('tree', node.tree),
                       contents=kwargs.get('contents', node.contents),
                       data=kwargs.get('data', node.data),
                       next=kwargs.get('next', node.next))
        elif 'children' in kwargs:
            return cls(tree=kwargs.get('tree', node.tree),
                       contents=kwargs.get('contents', node.contents),
                       children=kwargs.get('children', node.children))
        else:
            raise TypeError('Invalid parameters passed in.')

    def __repr__(self):
        name = 'Branch' if getattr(self, 'children', None) else 'Leaf'
        return '<{name} [{contents}]>'.format(
            name=name, contents=', '.join([str(i) for i in self.contents]))


class _BPlusBranch(_Node):
    __slots__ = ("tree", "contents", "children")

    def __init__(self, tree, contents=None, children=None):
        self.tree = tree
        self.contents = contents or list()
        self.children = children or list()
        if self.children:
            assert len(self.contents) + 1 == len(self.children), \
                "One more child than data item required."

    def split(self):
        center = len(self.contents) // 2
        median = self.contents[center]
        sibling = type(self)(tree=self.tree,
                             contents=self.contents[center + 1:],
                             children=self.children[center + 1:])
        self.contents = self.contents[:center]
        self.children = self.children[:center + 1]
        return sibling, median

    def grow(self, ancestors=None):
        parent, parent_index = ancestors.pop()

        minimum = math.ceil(self.tree.order / 2)
        left_sib = right_sib = None

        # try to borrow from the right sibling
        if parent_index + 1 < len(parent.children):
            right_sib = parent.children[parent_index + 1]
            if len(right_sib.contents) >= minimum:
                right_sib.lateral(parent, parent_index + 1, self, parent_index)
                return

        # try to borrow from the left sibling
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len(left_sib.contents) >= minimum:
                left_sib.lateral(parent, parent_index - 1, self, parent_index)
                return

        # consolidate with a sibling - try left first
        if left_sib:
            left_sib.contents.append(parent.contents[parent_index - 1])
            left_sib.contents.extend(self.contents)
            if self.children:
                left_sib.children.extend(self.children)
            parent.contents.pop(parent_index - 1)
            parent.children.pop(parent_index)
        else:
            self.contents.append(parent.contents[parent_index])
            self.contents.extend(right_sib.contents)
            if self.children:
                self.children.extend(right_sib.children)
            parent.contents.pop(parent_index)
            parent.children.pop(parent_index + 1)

        if len(parent.contents) < minimum:
            if ancestors:
                # parent is not the root
                parent.grow(ancestors)
            elif not parent.contents:
                # parent is root, and its now empty
                self.tree._root = left_sib or self

    def shrink(self, ancestors=None):
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

        sibling, median = self.split()

        if not parent:  # this is root node
            parent, parent_index = self.tree.BRANCH(
                tree=self.tree, children=[self]), 0
            self.tree._root = parent
        # pass the median up to the parent
        parent.contents.insert(parent_index, median)
        parent.children.insert(parent_index + 1, sibling)
        if len(parent.contents) > parent.tree.order:
            parent.shrink(ancestors)

    def lateral(self, parent, parent_index, dest, dest_index):
        if parent_index > dest_index:  # lend to the left neighboring sibling
            dest.contents.append(parent.contents[dest_index])
            parent.contents[dest_index] = self.contents.pop(0)
            if self.children:
                dest.children.append(self.children.pop(0))
        else:  # lend to the right neighboring sibling
            dest.contents.insert(0, parent.contents[parent_index])
            parent.contents[parent_index] = self.contents.pop()
            if self.children:
                dest.children.insert(0, self.children.pop())

    def insert(self, index, item, ancestors=None):
        self.contents.insert(index, item)
        if len(self.contents) > self.tree.order:
            self.shrink(ancestors)

    def remove(self, index, ancestors=None):
        minimum = math.ceil(self.tree.order/2)

        if self.children:
            # try promoting from the right subtree first,
            # but only if it won't have to resize
            additional_ancestors = [(self, index + 1)]
            descendant = self.children[index + 1]
            while hasattr(descendant, 'children'):
                additional_ancestors.append((descendant, 0))
                descendant = descendant.children[0]
            if len(descendant.contents) >= minimum:
                ancestors.extend(additional_ancestors)
                self.contents[index] = descendant.contents[0]
                descendant.remove(0, ancestors)
                return

            # fall back to the left child
            additional_ancestors = [(self, index)]
            descendant = self.children[index]
            while hasattr(descendant, 'children'):
                additional_ancestors.append(
                    (descendant, len(descendant.children) - 1))
                descendant = descendant.children[-1]
            ancestors.extend(additional_ancestors)
            self.contents[index] = descendant.contents[-1]
            descendant.remove(len(descendant.children) - 1, ancestors)
        else:
            self.contents.pop(index)
            if len(self.contents) < minimum and ancestors:
                self.grow(ancestors)


class _BPlusLeaf(_Node):
    __slots__ = ("tree", "contents", "data", "next")

    def __init__(self, tree, contents=None, data=None, next=None):
        self.tree = tree
        self.contents = contents or []
        self.data = data or []
        self.next = next  # point to the sibling
        assert len(self.contents) == len(self.data), "one data per key"

    def shrink(self, ancestors=None):
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
        # not returned.
        # which means self has to split and then rebuild.
        sibling, push = self.split()

        if not parent:
            parent, parent_index = self.tree.BRANCH(
                tree=self.tree, children=[self]), 0
            self.tree._root = parent

        # pass the median up to the parent
        parent.contents.insert(parent_index, push)
        parent.children.insert(parent_index + 1, sibling)
        if len(parent.contents) > parent.tree.order:
            parent.shrink(ancestors)

    def lateral(self, parent, parent_index, dest, dest_index):
        if parent_index > dest_index:  # lend to the left neighboring sibling
            dest.contents.append(self.contents.pop(0))
            dest.data.append(self.data.pop(0))
            parent.contents[dest_index] = self.contents[0]
        else:  # lend to the right
            dest.contents.insert(0, self.contents.pop())
            dest.data.insert(0, self.data.pop())
            parent.contents[parent_index] = dest.contents[0]

    def split(self):
        center = len(self.contents) // 2
        sibling = type(self)(tree=self.tree,
                             contents=self.contents[center:],
                             data=self.data[center:],
                             next=self.next)
        self.contents = self.contents[:center]
        self.data = self.data[:center]
        self.next = sibling
        return sibling, sibling.contents[0]

    def grow(self, ancestors):
        minimum = math.ceil(self.tree.order / 2)
        parent, parent_index = ancestors.pop()
        left_sib = right_sib = None

        # try borrowing from a neighbor - try right first
        if parent_index + 1 < len(parent.children):
            right_sib = parent.children[parent_index + 1]
            if len(right_sib.contents) >= minimum:
                right_sib.lateral(parent, parent_index + 1, self, parent_index)
                return

        # fallback to left
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len(left_sib.contents) >= minimum:
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

    def remove(self, index, ancestors):
        minimum = math.ceil(self.tree.order / 2)
        if index >= len(self.contents):
            self, index = self.next, 0

        key = self.contents[index]

        # if any leaf that could accept the key can do so
        # without any rebalancing necessity, then go that route
        current = self
        while current is not None and current.contents[0] == key:
            if len(current.contents) >= minimum or len(ancestors) == 0:
                if current.contents[0] == key:
                    index = 0
                else:
                    index = bisect.bisect_left(current.contents, key)
                current.contents.pop(index)
                current.data.pop(index)
                return
            current = current.next
        self.grow(ancestors)

    def insert(self, index, key, data, ancestors=None):
        self.contents.insert(index, key)
        self.data.insert(index, data)

        if len(self.contents) > self.tree.order:
            self.shrink(ancestors)


class BPlusTree(object):
    LEAF = _BPlusLeaf
    BRANCH = _BPlusBranch

    def __init__(self, order=100):
        self.order = order
        self._root = self._bottom = self.LEAF(self)

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

    def _path_to_branch(self, item):
        """
        :return: ancestors:list from root to item node (usually branch node)
        """
        current = self._root
        ancestry = []
        while hasattr(current, "children"):
            index = bisect.bisect_left(current.contents, item)
            ancestry.append((current, index))
            if index < len(current.contents) \
                    and current.contents[index] == item:
                return ancestry
            current = current.children[index]

        index = bisect.bisect_left(current.contents, item)
        ancestry.append((current, index))

        return ancestry

    def _path_to(self, item):
        """
        :return: the complete path from root to item node (leaf node)
        """
        path = self._path_to_branch(item)
        node, index = path[-1]
        while hasattr(node, "children"):
            node = node.children[index]
            index = bisect.bisect_left(node.contents, item)
            path.append((node, index))
        return path

    def get(self, key, default=None):
        try:
            return next(self._get(key))
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
        return "\n".join(accum)

    def iteritems(self):
        node = self._root
        while hasattr(node, "children"):
            node = node.children[0]

        while node:
            for pair in zip(node.contents, node.data):
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

