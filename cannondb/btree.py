import bisect
import math
from abc import ABCMeta
from typing import Iterable


class _BNode(metaclass=ABCMeta):
    __slots__ = ('tree', 'keys', 'values', 'children')

    def __init__(self, tree, keys=None, values=None, children=None):
        self.tree = tree
        self.keys = keys or []
        self.values = values or []
        self.children = children or []
        if self.children:
            assert len(self.keys) + 1 == len(self.children), \
                'One more child than data item required'

    def __repr__(self):
        name = 'Branch' if getattr(self, 'children', None) else 'Leaf'
        return '<{name} [{pairs}]>'.format(
            name=name, pairs=str(zip(self.keys, self.values)))

    def lateral(self, parent, parent_index, target, target_index):
        """
        lend one element from parent[parent_index] to target[target_index].
        """
        if parent_index > target_index:
            target.keys.append(parent.keys[target_index])
            target.values.append(parent.values[target_index])
            parent.keys[target_index] = self.keys.pop(0)
            parent.values[target_index] = self.values.pop(0)
            if self.children:
                target.children.append(self.children.pop(0))
        else:
            target.keys.insert(0, parent.keys[parent_index])
            target.values.insert(0, parent.values[parent_index])
            parent.keys[parent_index] = self.keys.pop()
            parent.values[parent_index] = self.values.pop()
            if self.children:
                target.children.insert(0, self.children.pop())

    def shrink(self, ancestors: list):
        """
        shrink from current node up to the root until tree is balanced.
        :param ancestors: ancestors from root to current node
        """
        parent = None

        if ancestors:
            parent, parent_index = ancestors.pop()
            # try to lend to the left neighboring sibling
            if parent_index:
                left_sib = parent.children[parent_index - 1]
                if len(left_sib.keys) < self.tree.order:
                    self.lateral(
                        parent, parent_index, left_sib, parent_index - 1)
                    return

            # try the right neighbor
            if parent_index + 1 < len(parent.children):
                right_sib = parent.children[parent_index + 1]
                if len(right_sib.keys) < self.tree.order:
                    self.lateral(
                        parent, parent_index, right_sib, parent_index + 1)
                    return

        sibling, mid_key, mid_val = self.split()

        if not parent:
            parent, parent_index = self.tree.BRANCH(
                self.tree, children=[self]), 0
            self.tree._root = parent

        # pass the median up to the parent
        parent.keys.insert(parent_index, mid_key)
        parent.values.insert(parent_index, mid_val)
        parent.children.insert(parent_index + 1, sibling)
        if len(parent.keys) > parent.tree.order:
            parent.shrink(ancestors)

    def grow(self, ancestors: list):
        """
        grow from current node up to the root until tree is balanced,
        by trying borrowing items from siblings or consolidate with siblings.
        :param ancestors: ancestors from root to current node
        """
        parent, parent_index = ancestors.pop()
        left_sib = right_sib = None
        # try to borrow from the right sibling
        if parent_index + 1 < len(parent.children):
            right_sib = parent.children[parent_index + 1]
            if len(right_sib.keys) > self.tree.min_elements:
                right_sib.lateral(parent, parent_index + 1, self, parent_index)
                return

        # try to borrow from the left sibling
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len(left_sib.keys) > self.tree.min_elements:
                left_sib.lateral(parent, parent_index - 1, self, parent_index)
                return

        # consolidate with a sibling - try left first
        if left_sib:
            left_sib.keys.append(parent.keys[parent_index - 1])
            left_sib.keys.extend(self.keys)
            left_sib.values.append(parent.values[parent_index - 1])
            left_sib.values.extend(self.values)
            if self.children:
                left_sib.children.extend(self.children)
            parent.keys.pop(parent_index - 1)
            parent.values.pop(parent_index - 1)
            parent.children.pop(parent_index)
        else:
            self.keys.append(parent.keys[parent_index])
            self.keys.extend(right_sib.keys)
            self.values.append(parent.keys[parent_index])
            self.values.extend(right_sib.values)
            if self.children:
                self.children.extend(right_sib.children)
            parent.keys.pop(parent_index)
            parent.values.pop(parent_index)
            parent.children.pop(parent_index + 1)

        if len(parent.keys) < self.tree.min_elements:
            if ancestors:
                # parent is not the root
                parent.grow(ancestors)
            elif not parent.keys:
                # parent is root, and it's now empty
                self.tree._root = left_sib or self

    def split(self):
        """
        split this node into two parts
        :returns: new node and median elements
        """
        center = len(self.keys) // 2
        mid_key = self.keys[center]
        mid_val = self.values[center]
        sibling = type(self)(
            self.tree,
            self.keys[center + 1:],
            self.values[center + 1:],
            self.children[center + 1:])
        self.keys = self.keys[:center]
        self.values = self.values[:center]
        self.children = self.children[:center + 1]
        return sibling, mid_key, mid_val

    def insert(self, index, key, value, ancestors):
        self.keys.insert(index, key)
        self.values.insert(index, value)
        if len(self.keys) > self.tree.order:
            self.shrink(ancestors)

    def remove(self, index, ancestors):

        if self.children:
            # try promoting from the right subtree first,
            # but only if it won't have to resize
            additional_ancestors = [(self, index + 1)]
            descendant = self.children[index + 1]
            while descendant.children:
                additional_ancestors.append((descendant, 0))
                descendant = descendant.children[0]
            if len(descendant.keys) > self.tree.min_elements:
                ancestors.extend(additional_ancestors)
                self.keys[index] = descendant.keys[0]
                self.values[index] = descendant.values[0]
                descendant.remove(0, ancestors)
                return

            # fall back to the left child
            additional_ancestors = [(self, index)]
            descendant = self.children[index]
            while descendant.children:
                additional_ancestors.append(
                    (descendant, len(descendant.children) - 1))
                descendant = descendant.children[-1]
            ancestors.extend(additional_ancestors)
            self.keys[index] = descendant.keys[-1]
            self.values[index] = descendant.values[-1]
            descendant.remove(len(descendant.children) - 1, ancestors)
        else:
            self.keys.pop(index)
            self.values.pop(index)
            if len(self.keys) < self.tree.min_elements and ancestors:
                self.grow(ancestors)


class BTree(object):
    BRANCH = LEAF = _BNode

    def __init__(self, order=100):
        self._order = order
        self._root = self._bottom = self.LEAF(self)
        self._count = 0

    def _path_to(self, key):
        """
        get the path from root to node which contains key.
        :return: list of node-path from root to key-node.
        """
        current = self._root
        ancestry = []

        while getattr(current, 'children', None):
            index = bisect.bisect_left(current.keys, key)
            ancestry.append((current, index))
            if index < len(current.keys) \
                    and current.keys[index] == key:
                return ancestry
            current = current.children[index]

        index = bisect.bisect_left(current.keys, key)
        ancestry.append((current, index))

        return ancestry

    @staticmethod
    def _present(key, ancestors) -> bool:
        """
        judge is key exist in this tree.
        """
        last, index = ancestors[-1]
        return index < len(last.keys) and last.keys[index] == key

    def insert(self, key, value, override=False):
        """
        :param key: key to be inserted
        :param value: value to be inserted corresponding to the key
        :param override: if override is true and key has existed, the new
                         value will override the old one.
        """
        ancestors = self._path_to(key)
        node, index = ancestors[-1]
        if BTree._present(key, ancestors):
            if not override:
                raise ValueError('{key} has existed'.format(key=key))
            else:
                node.values[index] = value
        else:
            while getattr(node, 'children', None):
                node = node.children[index]
                index = bisect.bisect_left(node.keys, key)
                ancestors.append((node, index))
            node, index = ancestors.pop()
            node.insert(index, key, value, ancestors)
        self._count += 1

    def multi_insert(self, pairs: Iterable, override=False):
        """
        insert a batch of key-value pair at one time.
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

    def remove(self, key):
        ancestors = self._path_to(key)

        if BTree._present(key, ancestors):
            node, index = ancestors.pop()
            node.remove(index, ancestors)
        else:
            raise ValueError('%r not in %s' % (key, self.__class__.__name__))
        self._count -= 1

    def _get(self, key):
        ancestor = self._path_to(key)
        node, index = ancestor[-1]
        if BTree._present(key, ancestor):
            yield node.values[index]
        else:
            raise StopIteration

    def get(self, key, default=None):
        """
        :param key: key expected to be searched in the tree.
        :param default: if key doesn't exist, return default.
        :return: value corresponding to the key if key exists.
        """
        try:
            return next(self._get(key))
        except StopIteration:
            return default

    def iteritems(self):
        for item in self:
            yield item

    def iterkeys(self):
        for key, _ in self:
            yield key

    def itervalues(self):
        for _, value in self:
            yield value

    def keys(self) -> list:
        return list(self.iterkeys())

    def values(self) -> list:
        return list(self.itervalues())

    def items(self) -> list:
        return list(self.iteritems())

    def __contains__(self, key):
        return BTree._present(key, self._path_to(key))

    def __iter__(self):
        def _recurse(node):
            if node.children:
                for child, it in zip(node.children, zip(node.keys, node.values)):
                    for child_item in _recurse(child):
                        yield child_item
                    yield it
                for child_item in _recurse(node.children[-1]):
                    yield child_item
            else:
                for it in zip(node.keys, node.values):
                    yield it

        for item in _recurse(self._root):
            yield item

    def __repr__(self):
        def recurse(node, all_items, depth):
            all_items.append((' ' * depth) + repr(node))
            for node in getattr(node, "children", list()):
                recurse(node, all_items, depth + 1)

        _all = list()
        recurse(self._root, _all, 0)
        return '\n'.join(_all)

    def __len__(self):
        return self.count

    def __setitem__(self, key, value):
        self.insert(key, value)

    __getitem__ = get
    __delitem__ = remove

    @classmethod
    def bulk_load(cls, key_val_pairs: Iterable, order=100):
        """
        build a complete B-tree by a set of key-value pairs.
        """
        tree = object.__new__(cls)
        tree.order = order

        leaves = tree._build_bulk_loaded_leaves(key_val_pairs)
        tree._build_bulk_loaded_branches(*leaves)

        return tree

    def _build_bulk_loaded_leaves(self, items):
        leaves, seps = [[]], []

        for item in items:
            if len(leaves[-1]) < self.order:
                leaves[-1].append(item)
            else:
                seps.append(item)
                leaves.append([])

        if len(leaves[-1]) < self.tree.min_elements and seps:
            last_two = leaves[-2] + [seps.pop()] + leaves[-1]
            leaves[-2] = last_two[:self.tree.min_elements]
            leaves[-1] = last_two[self.tree.min_elements + 1:]
            seps.append(last_two[self.tree.min_elements])

        return [self.LEAF(self, keys=node) for node in leaves], seps

    def _build_bulk_loaded_branches(self, leaves, seps):
        levels = [leaves]

        while len(seps) > self.order + 1:
            items, nodes, seps = seps, [[]], []

            for item in items:
                if len(nodes[-1]) < self.order:
                    nodes[-1].append(item)
                else:
                    seps.append(item)
                    nodes.append([])

            if len(nodes[-1]) < self.tree.min_elements and seps:
                last_two = nodes[-2] + [seps.pop()] + nodes[-1]
                nodes[-2] = last_two[:self.tree.min_elements]
                nodes[-1] = last_two[self.tree.min_elements + 1:]
                seps.append(last_two[self.tree.min_elements])

            offset = 0
            for i, node in enumerate(nodes):
                children = levels[-1][offset:offset + len(node) + 1]
                nodes[i] = self.BRANCH(self, keys=node, children=children)
                offset += len(node) + 1

            levels.append(nodes)

        self._root = self.BRANCH(self, keys=seps, children=levels[-1])

    @property
    def order(self):
        return self._order

    @order.setter
    def order(self, value):
        raise RuntimeError('order of b-tree is read only')

    @property
    def min_elements(self):
        return math.ceil(self.order / 2)

    @property
    def count(self):
        return self._count

    @count.setter
    def count(self, value):
        raise RuntimeError('count of b-tree elements is read-only')
