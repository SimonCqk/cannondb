import bisect
import math
from abc import ABCMeta


class _BNode(metaclass=ABCMeta):
    __slots__ = ("tree", "contents", "children")

    def __init__(self, tree, contents=None, children=None):
        self.tree = tree
        self.contents = contents or []
        self.children = children or []
        if self.children:
            assert len(self.contents) + 1 == len(self.children), \
                "one more child than data item required"

    def __repr__(self):
        name = 'Branch' if hasattr(self, 'children') else 'Leaf'
        return '<{name} [{contents}]>'.format(
            name=name, contents=', '.join([str(i) for i in self.contents]))

    def lateral(self, parent, parent_index, target, target_index):
        if parent_index > target_index:
            target.contents.append(parent.contents[target_index])
            parent.contents[target_index] = self.contents.pop(0)
            if self.children:
                target.children.append(self.children.pop(0))
        else:
            target.contents.insert(0, parent.contents[parent_index])
            parent.contents[parent_index] = self.contents.pop()
            if self.children:
                target.children.insert(0, self.children.pop())

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

    def grow(self, ancestors):
        parent, parent_index = ancestors.pop()

        left_sib = right_sib = None

        # try to borrow from the right sibling
        if parent_index + 1 < len(parent.children):
            right_sib = parent.children[parent_index + 1]
            if len(right_sib.contents) > self.tree.min_elements:
                right_sib.lateral(parent, parent_index + 1, self, parent_index)
                return

        # try to borrow from the left sibling
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len(left_sib.contents) > self.tree.min_elements:
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

        if len(parent.contents) < self.tree.min_elements:
            if ancestors:
                # parent is not the root
                parent.grow(ancestors)
            elif not parent.contents:
                # parent is root, and its now empty
                self.tree._root = left_sib or self

    def split(self):
        center = len(self.contents) // 2
        median = self.contents[center]
        sibling = type(self)(
            self.tree,
            self.contents[center + 1:],
            self.children[center + 1:])
        self.contents = self.contents[:center]
        self.children = self.children[:center + 1]
        return sibling, median

    def insert(self, index, item, ancestors):
        self.contents.insert(index, item)
        if len(self.contents) > self.tree.order:
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
            if len(descendant.contents) > self.tree.min_elements:
                ancestors.extend(additional_ancestors)
                self.contents[index] = descendant.contents[0]
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
            self.contents[index] = descendant.contents[-1]
            descendant.remove(len(descendant.children) - 1, ancestors)
        else:
            self.contents.pop(index)
            if len(self.contents) < self.tree.min_elements and ancestors:
                self.grow(ancestors)


class BTree(object):
    BRANCH = LEAF = _BNode

    def __init__(self, order=100):
        self._order = order
        self._root = self._bottom = self.LEAF(self)
        self._count = 0

    def _path_to(self, item):
        current = self._root
        ancestry = []

        while getattr(current, 'children', None):
            index = bisect.bisect_left(current.contents, item)
            ancestry.append((current, index))
            if index < len(current.contents) \
                    and current.contents[index] == item:
                return ancestry
            current = current.children[index]

        index = bisect.bisect_left(current.contents, item)
        ancestry.append((current, index))

        return ancestry

    @staticmethod
    def _present(item, ancestors):
        last, index = ancestors[-1]
        return index < len(last.contents) and last.contents[index] == item

    def insert(self, item, override=False):
        ancestors = self._path_to(item)
        node, index = ancestors[-1]
        if BTree._present(item, ancestors):
            if not override:
                raise ValueError('{key} has existed'.format(key=item))
            else:
                node[index] = item
        else:
            while getattr(node, "children", None):
                node = node.children[index]
                index = bisect.bisect_left(node.contents, item)
                ancestors.append((node, index))
            node, index = ancestors.pop()
            node.insert(index, item, ancestors)
        self._count += 1

    def remove(self, item):
        ancestors = self._path_to(item)

        if BTree._present(item, ancestors):
            node, index = ancestors.pop()
            node.remove(index, ancestors)
        else:
            raise ValueError('%r not in %s' % (item, self.__class__.__name__))
        self._count -= 1

    def __contains__(self, item):
        return BTree._present(item, self._path_to(item))

    def __iter__(self):
        def _recurse(node):
            if node.children:
                for child, it in zip(node.children, node.contents):
                    for child_item in _recurse(child):
                        yield child_item
                    yield it
                for child_item in _recurse(node.children[-1]):
                    yield child_item
            else:
                for it in node.contents:
                    yield it

        for item in _recurse(self._root):
            yield item

    def __repr__(self):
        def recurse(node, accum, depth):
            accum.append(("  " * depth) + repr(node))
            for node in getattr(node, "children", list()):
                recurse(node, accum, depth + 1)

        _accum = list()
        recurse(self._root, _accum, 0)
        return '\n'.join(_accum)

    '''
    @classmethod
    def bulkload(cls, items, order):
        tree = object.__new__(cls)
        tree.order = order

        leaves = tree._build_bulkloaded_leaves(items)
        tree._build_bulkloaded_branches(*leaves)

        return tree

    def _build_bulkloaded_leaves(self, items):
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

        return [self.LEAF(self, contents=node) for node in leaves], seps

    def _build_bulkloaded_branches(self, leaves, seps):
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
                nodes[i] = self.BRANCH(self, contents=node, children=children)
                offset += len(node) + 1

            levels.append(nodes)

        self._root = self.BRANCH(self, contents=seps, children=levels[-1])
    '''

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
