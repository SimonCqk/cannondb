import bisect
import logging
import math
from typing import Iterable

from cannondb.constants import TreeConf, DEFAULT_LOGGER_NAME
from cannondb.handler import FileHandler
from cannondb.node import BNode

logger = logging.getLogger(DEFAULT_LOGGER_NAME)


class DBNotOpenError(Exception):
    """raise when trying to do ops but storage was closed"""
    pass


class BTree(object):
    __slots__ = ('_file_name', '_order', '_root', '_bottom', '_tree_conf', 'handler', '_closed')
    BRANCH = LEAF = BNode

    def __init__(self, file_name: str = 'database', order=100, page_size: int = 8192, key_size: int = 16,
                 value_size: int = 64, cache_size=1024):
        self._file_name = file_name
        self._tree_conf = TreeConf(order=order, page_size=page_size,
                                   key_size=key_size, value_size=value_size)
        self.handler = FileHandler(file_name, self._tree_conf, cache_size=cache_size)
        self._order = order
        try:  # create new root or load previous root
            with self.handler.read_transaction:
                meta_root_page, meta_tree_conf = self.handler.get_meta_tree_conf()
        except ValueError:
            #  init empty tree
            with self.handler.write_transaction:
                self._root = self._bottom = self.LEAF(self, self._tree_conf)
                self.handler.ensure_root_block(self._root)
        else:
            with self.handler.read_transaction:
                self._root, self._tree_conf = self.handler.get_node(meta_root_page, tree=self), meta_tree_conf

        self._closed = False

    def _path_to(self, key):
        """
        get the path from root to node which contains _key.
        :return: list of node-path from root to _key-node.
        """
        with self.handler.read_transaction:
            current = self._root
            ancestry = []

            while getattr(current, 'children', None):
                index = bisect.bisect_left(current.contents, key)
                ancestry.append((current, index))
                if index < len(current.contents) \
                        and current.contents[index].key == key:
                    return ancestry
                current = self.handler.get_node(current.children[index], tree=self)

            index = bisect.bisect_left(current.contents, key)
            ancestry.append((current, index))

            return ancestry

    @staticmethod
    def _present(key, ancestors) -> bool:
        """
        judge is _key exist in this tree.
        """
        last, index = ancestors[-1]
        return index < len(last.contents) and last.contents[index].key == key

    def insert(self, key, value, override=False):
        """
        :param key: _key to be inserted
        :param value: _value to be inserted corresponding to the _key
        :param override: if override is true and _key has existed, the new
                         _value will override the old one.
        """
        ancestors = self._path_to(key)
        node, index = ancestors[-1]
        with self.handler.write_transaction:
            if BTree._present(key, ancestors):
                if not override:
                    raise ValueError('{key} has existed'.format(key=key))
                else:
                    node.contents[index].value = value
                    node.update_content_in_dump(index, node.contents[index])
                    self.handler.set_node(node)
            else:
                while getattr(node, 'children', None):
                    node = self.handler.get_node(node.children[index], tree=self)
                    index = bisect.bisect_left(node.contents, key)
                    ancestors.append((node, index))
                node, index = ancestors.pop()
                node.insert(index, key, value, ancestors)

    def multi_insert(self, pairs: Iterable, override=False):
        """
        insert a batch of _key-_value pair at one time.
        strongly recommend use multi insert when have a batch of pairs to insert,
        (commit one time) versus (commit n times).
        """
        with self.handler.write_transaction:
            if not isinstance(pairs, Iterable):
                raise TypeError('pairs should be a iterable object')
            elif isinstance(pairs, dict):
                for key, value in pairs.items():
                    self.insert(key, value, override)
            else:
                if hasattr(pairs, 'sort'):
                    pairs.sort(key=lambda p: p[0])  # sort by key
                for key, value in pairs:
                    self.insert(key, value, override)

    def multi_read(self, keys: Iterable) -> dict:
        """
        :param keys: keys need to read from database.
        :return: dictionary map from keys to values.
        """
        with self.handler.read_transaction:
            pairs = dict()
            for key in keys:
                pairs[key] = self.get(key)
        return pairs

    def remove(self, key):
        ancestors = self._path_to(key)

        if BTree._present(key, ancestors):
            node, index = ancestors.pop()
            with self.handler.write_transaction:
                node.remove(index, ancestors)
        else:
            raise KeyError('{key} not in {self}'.format(key=key, self=self.__class__.__name__))

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
        :return: _value corresponding to the key if key exists.
        """
        try:
            return next(self._get(key))
        except StopIteration:
            return default

    def _iteritems(self):
        for item in self:
            yield item

    def _iterkeys(self):
        for pair in self:
            yield pair[0]

    def _itervalues(self):
        for pair in self:
            yield pair[1]

    def keys(self) -> list:
        return list(self._iterkeys())

    def values(self) -> list:
        return list(self._itervalues())

    def items(self) -> list:
        return list(self._iteritems())

    def commit(self):
        self.handler.commit()

    def __contains__(self, key):
        return BTree._present(key, self._path_to(key))

    def __iter__(self):
        """
        support iterate B tree by yielding a _key-_value pair each time
        """

        def _recurse(node):
            if node.children:
                for child, it in zip(node.children, node.contents):
                    for child_item in _recurse(self.handler.get_node(child, tree=self)):
                        yield child_item
                    yield it.key, it.value
                for child_item in _recurse(self.handler.get_node(node.children[-1], tree=self)):
                    yield child_item
            else:
                for it in node.contents:
                    yield it.key, it.value

        with self.handler.read_transaction:
            for item in _recurse(self._root):
                yield item

    def __repr__(self):
        def recurse(node, all_items, depth):
            all_items.append((' ' * depth) + repr(node))
            for node in self.handler.get_node(getattr(node, 'children', list())):
                recurse(node, all_items, depth + 1)

        _all = list()
        recurse(self._root, _all, 0)
        return '\n'.join(_all)

    def __len__(self):
        return len([_ for _ in self])

    def __setitem__(self, key, value):
        self.insert(key, value, override=True)

    __getitem__ = get
    __delitem__ = remove

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def checkpoint(self):
        """manually perform checkpoint"""
        self.handler.perform_checkpoint(reopen_wal=True)

    def set_auto_commit(self, auto: bool):
        logging.info('Set database auto commit {state}.'.format(state='on' if auto else 'off'))
        self.handler._auto_commit = auto

    @property
    def next_available_page(self) -> int:
        return self.handler.next_available_page

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
    def is_open(self):
        return not self._closed

    def close(self):
        if self._closed:
            return
        with self.handler.write_transaction:
            self.handler.ensure_root_block(self._root)
            self._closed = True
        self.handler.close()
        logger.info('Database has been closed.')
