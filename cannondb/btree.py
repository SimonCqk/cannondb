import bisect
import io
import math
from typing import Iterable

import portalocker

from cannondb.constants import TreeConf
from cannondb.node import BNode, BaseBNode
from cannondb.utils import LRUCache, FakeCache, open_database_file, read_from_file, write_to_file


class PageOutOfRange(Exception):
    pass


class FileHandler(object):
    """Handling-layer between B tree engine and underlying db file"""
    __slots__ = ('_filename', '_tree_conf', '_cache', '_fd', '_lock', 'last_page')

    def __init__(self, file_name, tree_conf: TreeConf, cache_size=512):
        self._filename = file_name
        self._tree_conf = tree_conf

        if cache_size == 0:
            self._cache = FakeCache()
        else:
            self._cache = LRUCache(capacity=cache_size)
        self._fd = open_database_file(self._filename)
        self._lock = portalocker.lock(self._fd, portalocker.LOCK_EX)

        # Get the next available page
        self._fd.seek(0, io.SEEK_END)
        last_byte = self._fd.tell()
        self.last_page = int(last_byte / self._tree_conf.page_size)

    def _fd_seek_end(self):
        self._fd.seek(0, io.SEEK_END)

    def _get_page_data(self, page: int) -> bytes:
        page_start = page * self._tree_conf.page_size

        data = read_from_file(self._fd, page_start,
                              page_start + self._tree_conf.page_size)
        if data == b'':
            raise PageOutOfRange('Page index out of range')
        else:
            return data

    def _set_page_data(self, page: int, page_data: bytes):
        assert len(page_data) == self._tree_conf.page_size
        self._fd.seek(page * self._tree_conf.page_size)
        write_to_file(self._fd, page_data)

    @property
    def next_available_page(self) -> int:
        self.last_page += 1
        return self.last_page

    def set_node(self, node: BNode):
        self._set_page_data(node.page, node.dump())
        self._cache[node.page] = node

    def get_node(self, page: int):
        node = self._cache.get(page)
        if node:
            return node
        data = self._get_page_data(page)
        node = BaseBNode.from_raw_data(self._tree_conf, page, data)
        self._cache[node.page] = node
        return node

    def _ensure_root_block(self):
        pass


class BTree(object):
    __slots__ = ('_order', '_count', '_root', '_bottom')
    BRANCH = LEAF = BNode

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
            index = bisect.bisect_left(current.contents, key)
            ancestry.append((current, index))
            if index < len(current.contents) \
                    and current.contents[index].key == key:
                return ancestry
            current = current.children[index]

        index = bisect.bisect_left(current.contents, key)
        ancestry.append((current, index))

        return ancestry

    @staticmethod
    def _present(key, ancestors) -> bool:
        """
        judge is key exist in this tree.
        """
        last, index = ancestors[-1]
        return index < len(last.contents) and last.contents[index].key == key

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
                node.contents[index].value = value
        else:
            while getattr(node, 'children', None):
                node = node.children[index]
                index = bisect.bisect_left(node.contents, key)
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
            yield node.contents[index].value
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
                for child, it in zip(node.children, node.contents):
                    for child_item in _recurse(child):
                        yield child_item
                    yield {it.key: it.val}
                for child_item in _recurse(node.children[-1]):
                    yield child_item
            else:
                for it in node.contents:
                    yield {it.key: it.val}

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

    @property
    def next_available_page(self) -> int:
        pass

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
