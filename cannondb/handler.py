import bisect
import enum
import io
import logging
import os
from typing import Union

import rwlock

from cannondb.constants import *
from cannondb.node import BNode, BaseBNode, OverflowNode
from cannondb.utils import LRUCache, FakeCache, open_database_file, read_from_file, write_to_file, \
    file_flush_and_sync, EndOfFileError

logger = logging.getLogger(__name__)


class FileHandler(object):
    """
    Handling-layer between B tree engine and underlying db file
    """
    __slots__ = ('_filename', '_tree_conf', '_cache', '_fd', '_wal', '_lock',
                 'last_page', '_page_GC', 'auto_commit')

    def __init__(self, file_name, tree_conf: TreeConf, cache_size=256):
        self._filename = file_name
        self._tree_conf = tree_conf

        if cache_size == 0:
            self._cache = FakeCache()
        else:
            self._cache = LRUCache(capacity=cache_size)
        self._fd = open_database_file(self._filename)
        self._lock = rwlock.RWLock()
        self._wal = WAL(file_name, tree_conf.page_size)

        # Get the next available page
        self._fd.seek(0, io.SEEK_END)
        last_byte = self._fd.tell()
        self.last_page = int(last_byte / self._tree_conf.page_size)
        self._page_GC = list(self._load_page_gc())
        self.auto_commit = True

    @property
    def write_transaction(self):
        class WriteTransaction:
            def __enter__(_self):
                self._lock.writer_lock.acquire()

            def __exit__(_self, exc_type, exc_val, exc_tb):
                # When some emergency happens in the middle of a write
                # transaction we must roll it back and clear the cache
                # because the writer may have partially modified the Nodes
                if exc_type:
                    self._wal.rollback()
                    self._cache.clear()
                else:
                    if self.auto_commit:
                        self._wal.commit()
                self._lock.writer_lock.release()

        return WriteTransaction()

    @property
    def read_transaction(self):
        class ReadTransaction:
            def __enter__(_self):
                self._lock.reader_lock.acquire()

            def __exit__(_self, exc_type, exc_val, exc_tb):
                self._lock.reader_lock.release()

        return ReadTransaction()

    def _fd_seek_end(self):
        self._fd.seek(0, io.SEEK_END)

    def _read_page_data(self, page: int) -> bytes:
        """
        read No.page overflow_data from db file
        """
        page_start = page * self._tree_conf.page_size
        data = read_from_file(self._fd, page_start,
                              page_start + self._tree_conf.page_size)
        return data

    def _write_page_data(self, page: int, page_data: bytes, f_sync=False):
        """
        write page overflow_data to position No.page in db file, call system sync if specified f_sync,
        but sync is an expensive operation.
        """
        assert len(page_data) == self._tree_conf.page_size, 'length of page data does not match page size'
        page_start = page * self._tree_conf.page_size
        self._fd.seek(page_start)
        write_to_file(self._fd, page_data, f_sync=f_sync)

    def _load_page_gc(self):
        """load all deprecated pages used before"""
        for offset in range(1, self.last_page):
            page_start = offset * self._tree_conf.page_size
            page_type = read_from_file(self._fd, page_start, page_start + NODE_TYPE_LENGTH_LIMIT)
            if page_type == 2:  # _PageType.DEPRECATED_PAGE._value==2
                yield offset

    def collect_deprecated_page(self, page: int):
        """add new deprecated page to GC, smaller first"""
        bisect.insort_left(self._page_GC, page)

    def set_deprecated_data(self, dep_page: int, dep_page_data: bytes):
        """
        set page as deprecated in db file
        :param dep_page: page to be set as deprecated
        :param dep_page_data: only deprecated type as bytes is required
        """
        if dep_page in self._cache:  # remove deprecated node in cache
            del self._cache[dep_page]
        self._fd.seek(dep_page * self._tree_conf.page_size)
        write_to_file(self._fd, dep_page_data)

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

    def perform_checkpoint(self, reopen_wal=False):
        logger.info('Performing checkpoint of {name}'.format(name=self._filename))
        for page, page_data in self._wal.checkpoint():
            self._write_page_data(page, page_data, f_sync=False)
        file_flush_and_sync(self._fd)
        if reopen_wal:
            self._wal = WAL(self._filename, self._tree_conf.page_size)

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
        #self._wal.set_page(node.page, node.dump())
        self._write_page_data(node.page, node.dump())
        self._cache[node.page] = node

    def get_node(self, page: int, tree):
        """
        try get node from cache to avoid IO op, if not exist, read and load from db file
        """
        node = self._cache.get(page)
        if node:
            return node

        #data = self._wal.get_page(page)
        #if not data:
        data = self._read_page_data(page)

        node = BaseBNode.from_raw_data(tree, self._tree_conf, page, data)
        self._cache[node.page] = node
        return node

    def ensure_root_block(self, root: BNode):
        """sync root node information with both memory and disk"""
        self.set_node(root)
        self.set_meta_tree_conf(root.page, root.tree_conf)

    def commit(self):
        """sync uncommitted changes with db file"""
        self._wal.commit()

    def rollback(self):
        self._wal.rollback()

    def flush(self):
        """flush uncommitted changes to db file then clear the cache"""
        # can not just iterate cache, because during iterating,
        # cache will do refresh and size of cache changed at run time.
        with self.write_transaction:
            nodes = [node for node in self._cache.values()]
            self._cache.clear()
            for node in nodes:
                self._wal.set_page(node.page, node.dump())
            self.commit()
            self.perform_checkpoint(reopen_wal=True)
            file_flush_and_sync(self._fd)

    def close(self):
        self.perform_checkpoint()
        self._fd.close()
        self._cache.clear()


class FrameType(enum.Enum):
    PAGE = 1
    COMMIT = 2
    ROLLBACK = 3


class WAL:
    __slots__ = ('filename', '_fd', '_page_size', '_committed_pages', '_not_committed_pages', 'needs_recovery')

    FRAME_HEADER_LENGTH = FRAME_TYPE_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT

    def __init__(self, filename: str, page_size: int):
        self.filename = filename
        self._fd = open_database_file(file_name=filename, suffix='.cdb.wal')
        self._page_size = page_size
        self._committed_pages = dict()
        self._not_committed_pages = dict()

        self._fd.seek(0, io.SEEK_END)
        if self._fd.tell() == 0:
            self._create_header()
            self.needs_recovery = False
        else:
            logger.warning('Found an existing WAL file, '
                           'the B+Tree was not closed properly')
            self.needs_recovery = True
            self._load_wal()

    def checkpoint(self):
        """Transfer the modified data back to the tree and close the WAL."""
        if self._not_committed_pages:
            logger.warning('Closing WAL with uncommitted data, discarding it')

        file_flush_and_sync(self._fd)

        for page, page_start in self._committed_pages.items():
            page_data = read_from_file(
                self._fd,
                page_start,
                page_start + self._page_size
            )
            yield page, page_data

        self._fd.close()
        os.unlink(self.filename + '.cdb.wal')

    def _create_header(self):
        data = self._page_size.to_bytes(PAGE_LENGTH_LIMIT, ENDIAN)
        self._fd.seek(0)
        write_to_file(self._fd, data, f_sync=True)

    def _load_wal(self):
        self._fd.seek(0)
        header_data = read_from_file(self._fd, 0, PAGE_LENGTH_LIMIT)
        assert int.from_bytes(header_data, ENDIAN) == self._page_size

        while True:
            try:
                self._load_next_frame()
            except EndOfFileError:
                break
        if self._not_committed_pages:
            logger.warning('WAL has uncommitted data, discarding it')
            self._not_committed_pages = dict()

    def _load_next_frame(self):
        start = self._fd.tell()
        stop = start + self.FRAME_HEADER_LENGTH
        data = read_from_file(self._fd, start, stop)

        frame_type = int.from_bytes(data[0:FRAME_TYPE_LENGTH_LIMIT], ENDIAN)
        page = int.from_bytes(
            data[FRAME_TYPE_LENGTH_LIMIT:FRAME_TYPE_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT],
            ENDIAN
        )

        frame_type = FrameType(frame_type)
        if frame_type is FrameType.PAGE:
            self._fd.seek(stop + self._page_size)

        self._index_frame(frame_type, page, stop)

    def _index_frame(self, frame_type: FrameType, page: int, page_start: int):
        if frame_type is FrameType.PAGE:
            self._not_committed_pages[page] = page_start
        elif frame_type is FrameType.COMMIT:
            self._committed_pages.update(self._not_committed_pages)
            self._not_committed_pages = dict()
        elif frame_type is FrameType.ROLLBACK:
            self._not_committed_pages = dict()
        else:
            assert False

    def _add_frame(self, frame_type: FrameType, page: int = None,
                   page_data: bytes = None):
        if frame_type is FrameType.PAGE and (not page or not page_data):
            raise ValueError('PAGE frame without page data')
        if page_data and len(page_data) != self._page_size:
            raise ValueError('Page data is different from page size')
        if not page:
            page = 0
        if frame_type is not FrameType.PAGE:
            page_data = b''
        data = (
                frame_type.value.to_bytes(FRAME_TYPE_LENGTH_LIMIT, ENDIAN) +
                page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN) +
                page_data
        )

        if page in self._not_committed_pages.keys() or page in self._committed_pages.keys() and \
                frame_type == FrameType.PAGE:
            # if page has wrote before, overwrite it, or the size of .wal file will be boom.
            page_start = self._not_committed_pages.get(page, None) or self._committed_pages.get(page)
            self._fd.seek(page_start)
        else:
            self._fd.seek(0, io.SEEK_END)
        write_to_file(self._fd, data, f_sync=frame_type != FrameType.PAGE)
        self._index_frame(frame_type, page, self._fd.tell() - self._page_size)

    def get_page(self, page: int) -> bytes:
        page_start = None
        for store in (self._not_committed_pages, self._committed_pages):
            page_start = store.get(page)
            if page_start:
                break

        if not page_start:
            return b''

        return read_from_file(self._fd, page_start,
                              page_start + self._page_size)

    def set_page(self, page: int, page_data: bytes):
        self._add_frame(FrameType.PAGE, page, page_data)

    def commit(self):
        # Commit is a no-op when there is no uncommitted pages
        if self._not_committed_pages:
            self._add_frame(FrameType.COMMIT)

    def rollback(self):
        # Rollback is a no-op when there is no uncommitted pages
        if self._not_committed_pages:
            self._add_frame(FrameType.ROLLBACK)

    def __repr__(self):
        return '<WAL: {}>'.format(self.filename)
