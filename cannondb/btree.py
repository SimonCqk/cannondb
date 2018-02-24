import bisect
import io
import math
from typing import Iterable, Union

import rwlock

from cannondb.constants import *
from cannondb.node import BNode, BaseBNode, OverflowNode
from cannondb.utils import LRUCache, FakeCache, open_database_file, read_from_file, write_to_file, \
    file_flush_and_sync, EndOfFileError


class DBNotOpenError(Exception):
    """raise when trying to do ops but storage was closed"""


class FileHandler(object):
    """
    Handling-layer between B tree engine and underlying db file
    """
    __slots__ = ('_filename', '_tree_conf', '_cache', '_fd', '_lock', 'last_page',
                 '_committed_pages', '_uncommitted_pages', '_page_GC')

    def __init__(self, file_name, tree_conf: TreeConf, cache_size=256):
        self._filename = file_name
        self._tree_conf = tree_conf

        if cache_size == 0:
            self._cache = FakeCache()
        else:
            self._cache = LRUCache(capacity=cache_size)
        self._fd = open_database_file(self._filename)
        self._lock = rwlock.RWLock()

        # Get the next available page
        self._fd.seek(0, io.SEEK_END)
        last_byte = self._fd.tell()
        self.last_page = int(last_byte / self._tree_conf.page_size)
        self._committed_pages = dict()
        self._uncommitted_pages = dict()
        self._page_GC = list(self._load_page_gc())

    @property
    def write_lock(self):
        class WriteLock:
            def __enter__(_self):
                self._lock.writer_lock.acquire()

            def __exit__(_self, exc_type, exc_val, exc_tb):
                if exc_type:
                    self._cache.clear()
                    self.rollback()
                else:
                    pass
                self._lock.writer_lock.release()

        return WriteLock()

    @property
    def read_lock(self):
        class ReadLock:
            def __enter__(_self):
                self._lock.reader_lock.acquire()

            def __exit__(_self, exc_type, exc_val, exc_tb):
                self._lock.reader_lock.release()

        return ReadLock()

    def _fd_seek_end(self):
        self._fd.seek(0, io.SEEK_END)

    def _read_page_data(self, page: int) -> bytes:
        """
        read No.page overflow_data from db file
        """
        page_start = page * self._tree_conf.page_size
        data = read_from_file(self._fd, page_start,
                              page_start + self._tree_conf.page_size)
        if data == b'':
            raise EndOfFileError('Page index out of range or page overflow_data noe set yet')
        else:
            return data

    def _write_page_data(self, page: int, page_data: bytes, f_sync=False):
        """
        write page overflow_data to position No.page in db file, call system sync if specified f_sync,
        but sync is an expensive operation.
        """
        assert len(page_data) == self._tree_conf.page_size, 'length of page overflow_data does not match page size'
        page_start = page * self._tree_conf.page_size
        self._uncommitted_pages[page] = page_start
        self._fd.seek(page_start)
        write_to_file(self._fd, page_data, f_sync=f_sync)

    def _load_page_gc(self):
        """load all deprecated pages used before"""
        for offset in range(1, self.last_page):
            page_start = offset * self._tree_conf.page_size
            page_type = read_from_file(self._fd, page_start, page_start + NODE_TYPE_LENGTH_LIMIT)
            if page_type == 2:  # _PageType.DEPRECATED_PAGE.value==2
                yield offset

    def collect_deprecated_page(self, page: int):
        """add new deprecated page to GC, smaller first"""
        bisect.insort_left(self._page_GC, page)

    def _takeout_deprecated_page(self):
        """if GC has more than one page, take out smallest one"""
        if self._page_GC:
            return self._page_GC.pop(0)
        return None

    def set_meta_tree_conf(self, root_page: int, tree_conf: TreeConf):
        """
        set current tree configuration into db file, recorded by first page.
        file sync is necessary.
        """
        self._tree_conf = tree_conf
        length = PAGE_ADDRESS_LIMIT + 1 + PAGE_LENGTH_LIMIT + KEY_LENGTH_LIMIT + VALUE_LENGTH_LIMIT
        data = (
                root_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN) +
                self._tree_conf.order.to_bytes(1, ENDIAN) +
                self._tree_conf.page_size.to_bytes(PAGE_LENGTH_LIMIT, ENDIAN) +
                self._tree_conf.key_size.to_bytes(KEY_LENGTH_LIMIT, ENDIAN) +
                self._tree_conf.value_size.to_bytes(VALUE_LENGTH_LIMIT, ENDIAN) +
                bytes(self._tree_conf.page_size - length)  # padding
        )
        self._write_page_data(0, data, f_sync=True)

    def get_meta_tree_conf(self) -> tuple:
        """
        read former recorded tree configuration from db file, first page
        """
        try:
            data = self._read_page_data(0)
        except EndOfFileError:
            raise ValueError('Meta tree configure overflow_data has not set yet')
        root_page = int.from_bytes(data[0:PAGE_ADDRESS_LIMIT], ENDIAN)
        order_end = PAGE_ADDRESS_LIMIT + 1
        order = int.from_bytes(data[PAGE_ADDRESS_LIMIT:order_end], ENDIAN)
        page_size_end = order_end + PAGE_LENGTH_LIMIT
        page_size = int.from_bytes(data[order_end:page_size_end], ENDIAN)
        key_size_end = page_size_end + KEY_LENGTH_LIMIT
        key_size = int.from_bytes(data[page_size_end:key_size_end], ENDIAN)
        value_size_end = key_size_end + VALUE_LENGTH_LIMIT
        value_size = int.from_bytes(data[key_size_end:value_size_end], ENDIAN)
        if order != self._tree_conf.order:
            order = self._tree_conf.order
        self._tree_conf = TreeConf(order, page_size, key_size, value_size)
        return root_page, self._tree_conf

    @property
    def next_available_page(self) -> int:
        """try get one page from page GC (deprecated pages), else get by increase total pages"""
        dep_page = self._takeout_deprecated_page()
        if dep_page:
            return dep_page
        else:
            self.last_page += 1
            return self.last_page

    def set_node(self, node: Union[BNode, OverflowNode]):
        """
        add & update node overflow_data into db file and also add to cache
        """
        self._write_page_data(node.page, node.dump())
        self._cache[node.page] = node

    def get_node(self, page: int, tree):
        """
        try get node from cache to avoid IO op, if not exist, read and load from db file
        """
        node = self._cache.get(page)
        if node:
            return node
        data = self._read_page_data(page)
        node = BaseBNode.from_raw_data(tree, self._tree_conf, page, data)
        self._cache[node.page] = node
        return node

    def ensure_root_block(self, root: BNode):
        """sync root node information with both memory and disk"""
        with self.write_lock:
            self.set_node(root)
            self.set_meta_tree_conf(root.page, root.tree_conf)

    def commit(self):
        """sync uncommitted changes with db file"""
        if self._uncommitted_pages:
            self._committed_pages.update(self._uncommitted_pages)
            self._uncommitted_pages.clear()
        self.flush()

    def rollback(self):
        if self._uncommitted_pages:
            self._uncommitted_pages.clear()

    def flush(self):
        """flush uncommitted changes to db file then clear the cache"""
        with self.write_lock:
            for node_page, node in self._cache.items():
                self._write_page_data(node_page, node.dump())
            file_flush_and_sync(self._fd)
            self._cache.clear()

    def close(self):
        self.commit()
        self._fd.close()


class BTree(object):
    __slots__ = ('_file_name', '_order', '_root', '_bottom', '_tree_conf', 'handler', '_closed')
    BRANCH = LEAF = BNode

    def __init__(self, file_name: str = 'database', order=100, page_size: int = 4096, key_size: int = 16,
                 value_size: int = 64, cache_size=128):
        self._file_name = file_name
        self._tree_conf = TreeConf(order=order, page_size=page_size,
                                   key_size=key_size, value_size=value_size)
        self.handler = FileHandler(file_name, self._tree_conf, cache_size=cache_size)
        self._order = order
        try:  # create new root or load previous root
            meta_root_page, meta_tree_conf = self.handler.get_meta_tree_conf()
        except ValueError:
            #  init empty tree
            self._root = self._bottom = self.LEAF(self, self._tree_conf)
            self.handler.ensure_root_block(self._root)
        else:
            self._root, self._tree_conf = self.handler.get_node(meta_root_page, tree=self), meta_tree_conf

        self._closed = False

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
            current = self.handler.get_node(current.children[index], tree=self)

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
        :return: value corresponding to the key if key exists.
        """
        try:
            return next(self._get(key))
        except StopIteration:
            return default

    def _iteritems(self):
        for item in self:
            yield item

    def _iterkeys(self):
        for key, _ in self:
            yield key

    def _itervalues(self):
        for _, value in self:
            yield value

    def keys(self) -> list:
        return list(self._iterkeys())

    def values(self) -> list:
        return list(self._itervalues())

    def items(self) -> list:
        return list(self._iteritems())

    def __contains__(self, key):
        return BTree._present(key, self._path_to(key))

    def __iter__(self):
        """
        support iterate B tree by yielding a key-value pair each time
        """

        def _recurse(node):
            if node.children:
                for child, it in zip(node.children, node.contents):
                    for child_item in _recurse(self.handler.get_node(child, tree=self)):
                        yield child_item
                    yield {it.key: it.value}
                for child_item in _recurse(self.handler.get_node(node.children[-1], tree=self)):
                    yield child_item
            else:
                for it in node.contents:
                    yield {it.key: it.value}

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
        self.handler.ensure_root_block(self._root)
        self.handler.close()
        self._closed = True
