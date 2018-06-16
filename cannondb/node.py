import enum
import functools
import struct
from abc import ABCMeta, abstractmethod

from cannondb.constants import *
from cannondb.serializer import serializer_switcher, type_switcher


@functools.total_ordering
class KeyValPair(metaclass=ABCMeta):
    """
    Unit stores a pair of key-value, switch its serializer automatically by its type.
    """
    __slots__ = ('_key', '_value', 'length', 'tree_conf', 'key_ser', 'val_ser', '_dumped')

    def __init__(self, tree_conf: TreeConf, key=None, value=None, data: bytes = None):
        self.tree_conf = tree_conf
        self._key = key
        self._value = value
        self.length = (KEY_LENGTH_LIMIT + self.tree_conf.key_size +
                       VALUE_LENGTH_LIMIT + self.tree_conf.value_size +
                       2 * SERIALIZER_TYPE_LENGTH_LIMIT)
        if self._key is not None and self._value is not None:
            self.key_ser = serializer_switcher(type(key))
            self.val_ser = serializer_switcher(type(value))
        if data:
            self.load(data)
        self._dumped = None

    def load(self, data: bytes):
        assert len(data) == self.length
        key_len_end = KEY_LENGTH_LIMIT
        key_len = struct.unpack(KEY_LENGTH_FORMAT, data[0:key_len_end])[0]

        assert 0 <= key_len <= self.tree_conf.key_size

        key_end = key_len_end + key_len
        key_type_start = key_len_end + self.tree_conf.key_size
        key_type_end = key_type_start + SERIALIZER_TYPE_LENGTH_LIMIT
        self.key_ser = serializer_switcher(type_switcher(int.from_bytes(data[key_type_start:key_type_end], ENDIAN)))
        self._key = self.key_ser.deserialize(data[key_len_end:key_end])

        val_len_end = key_type_end + VALUE_LENGTH_LIMIT
        val_len = struct.unpack(VALUE_LENGTH_FORMAT, data[key_type_end:val_len_end])[0]

        assert 0 <= val_len <= self.tree_conf.value_size

        val_end = val_len_end + val_len
        val_type_start = val_len_end + self.tree_conf.value_size
        val_type_end = val_type_start + SERIALIZER_TYPE_LENGTH_LIMIT
        self.val_ser = serializer_switcher(type_switcher(int.from_bytes(data[val_type_start:val_type_end], ENDIAN)))
        self._value = self.val_ser.deserialize(data[val_len_end:val_end])

    def _dump(self):
        key_as_bytes = self.key_ser.serialize(self._key)
        key_len = len(key_as_bytes)
        key_type_as_bytes = type_switcher(type(self._key)).to_bytes(SERIALIZER_TYPE_LENGTH_LIMIT, ENDIAN)
        val_as_bytes = self.val_ser.serialize(self._value)
        val_len = len(val_as_bytes)
        val_type_as_bytes = type_switcher(type(self._value)).to_bytes(SERIALIZER_TYPE_LENGTH_LIMIT, ENDIAN)
        data = (
                struct.pack(KEY_LENGTH_FORMAT, key_len) +
                key_as_bytes +
                bytes(self.tree_conf.key_size - key_len) +
                key_type_as_bytes +
                struct.pack(VALUE_LENGTH_FORMAT, val_len) +
                val_as_bytes +
                bytes(self.tree_conf.value_size - val_len) +
                val_type_as_bytes
        )
        self._dumped = bytearray(data)

    def dump(self) -> bytes:
        # assert self._key is not None and self._value is not None
        if self._dumped:
            return bytes(self._dumped)
        self._dump()
        return bytes(self._dumped)

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, new_key):
        self._key = new_key
        if self._dumped:
            key_as_bytes = self.key_ser.serialize(self._key)
            key_len = len(key_as_bytes)
            key_as_bytes += bytes(self.tree_conf.key_size - key_len)
            self._dumped[0:KEY_LENGTH_LIMIT] = struct.pack(KEY_LENGTH_FORMAT, key_len)
            self._dumped[KEY_LENGTH_LIMIT:KEY_LENGTH_LIMIT + self.tree_conf.key_size] = key_as_bytes

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_val):
        self._value = new_val
        if self._dumped:
            val_as_bytes = self.val_ser.serialize(self._value)
            val_len = len(val_as_bytes)
            val_as_bytes += bytes(self.tree_conf.value_size - val_len)
            val_len_start = KEY_LENGTH_LIMIT + self.tree_conf.key_size + SERIALIZER_TYPE_LENGTH_LIMIT
            self._dumped[val_len_start:val_len_start + VALUE_LENGTH_LIMIT] = struct.pack(VALUE_LENGTH_FORMAT, val_len)
            val_start = val_len_start + VALUE_LENGTH_LIMIT
            self._dumped[val_start:val_start + self.tree_conf.value_size] = val_as_bytes

    def __eq__(self, other):
        if isinstance(other, KeyValPair):
            return self._key == other._key and self._value == self._value
        return self._key == other

    def __lt__(self, other):
        return self._key < other

    def __str__(self):
        return '<{key}:{val}>'.format(key=self._key, val=self._value)

    __repr__ = __str__


class _PageType(enum.Enum):
    NORMAL_PAGE = 0
    OVERFLOW_PAGE = 1
    DEPRECATED_PAGE = 2


class BaseBNode(metaclass=ABCMeta):
    PAGE_TYPE = None

    @abstractmethod
    def load(self, data: bytes):
        """create node from raw overflow_data"""
        pass

    @abstractmethod
    def dump(self) -> bytes:
        """convert node to bytes which contains all information of this node"""
        pass

    @classmethod
    def from_raw_data(cls, tree, tree_conf: TreeConf, page: int, data: bytes):
        """construct node from raw data, corresponding to it's node type"""
        # assert len(data) == tree_conf.page_size
        node_type = int.from_bytes(data[0:NODE_TYPE_LENGTH_LIMIT], ENDIAN)
        if node_type == 0:
            return BNode(tree, tree_conf, page=page, data=data)
        elif node_type == 1:
            return OverflowNode(tree, tree_conf, page=page, data=data)
        elif node_type == 2:
            raise TypeError('Deprecated pages can only be used by pages-GC.')
        else:
            raise TypeError('No such node type:{type} matched'.format(type=node_type))

    def _create_or_update_overflow(self, data: bytes, header_len: int) -> bytes:
        """
        if has created overflow page before, update it, else create new
        :return: cropped origin-data
        """
        detach_start = self.tree_conf.page_size - header_len
        if self.next_page:
            # has created overflow page, update it.
            of = self.tree.handler.get_node(self.next_page, tree=self.tree)
        else:
            # create new overflow page
            self.next_page = self.tree.next_available_page
            of = OverflowNode(tree=self.tree, tree_conf=self.tree_conf, page=self.next_page, parent_page=self.page)
        of.update_overflow_data(data[detach_start:])
        of.flush()
        return data[0:detach_start]


class OverflowNode(BaseBNode):
    """
    Recording overflow pages' information and raw overflow_data
    """
    __slots__ = ('tree', 'tree_conf', 'page', 'parent_page', 'next_page', 'overflow_data', '_dumped')
    PAGE_TYPE = _PageType.OVERFLOW_PAGE

    def __init__(self, tree, tree_conf: TreeConf, page: int, parent_page: int = None, next_page: int = None,
                 data: bytes = None):
        self.tree = tree
        self.tree_conf = tree_conf
        self.page = page
        self.parent_page = parent_page
        self.next_page = next_page
        self.overflow_data = None
        if data:
            self.load(data)
        self._dumped = None  # internal dump cache. Re-dump every time is extremely expensive.

    def load(self, data: bytes):
        # assert len(data) == self.tree_conf.page_size
        node_type = int.from_bytes(data[0:NODE_TYPE_LENGTH_LIMIT], ENDIAN)
        assert node_type == self.PAGE_TYPE.value
        data_len_end = NODE_TYPE_LENGTH_LIMIT + PAGE_LENGTH_LIMIT
        data_len = int.from_bytes(data[NODE_TYPE_LENGTH_LIMIT:data_len_end], ENDIAN)
        header_end = data_len_end + PAGE_ADDRESS_LIMIT
        self.next_page = int.from_bytes(data[data_len_end:header_end], ENDIAN)
        if self.next_page == 0:
            self.next_page = None
        self.overflow_data = data[header_end:header_end + data_len]

    def dump(self) -> bytes:
        if self._dumped:
            return bytes(self._dumped)
        data = bytearray()
        header_len = NODE_TYPE_LENGTH_LIMIT + PAGE_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT
        if len(self.overflow_data) + header_len > self.tree_conf.page_size:  # overflow
            self.overflow_data = self._create_or_update_overflow(self.overflow_data, header_len)
            assert len(self.overflow_data) == self.tree_conf.page_size - header_len
        elif len(self.overflow_data) + header_len <= self.tree_conf.page_size and self.next_page:
            # overflow before, but normal currently
            self.tree.handler.get_node(self.next_page, tree=self.tree).set_as_deprecated()
            self.next_page = None
        data.extend(self.overflow_data)
        next_page = 0 if self.next_page is None else self.next_page
        header = (
                self.PAGE_TYPE.value.to_bytes(NODE_TYPE_LENGTH_LIMIT, ENDIAN) +
                len(self.overflow_data).to_bytes(PAGE_LENGTH_LIMIT, ENDIAN) +
                next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
        )
        data = bytearray(header) + data
        if len(data) < self.tree_conf.page_size:
            padding = bytearray(self.tree_conf.page_size - len(data))
            data.extend(padding)
        self._dumped = data
        return bytes(self._dumped)

    def flush(self):
        """
        write overflow overflow_data into file
        """
        self.tree.handler.set_node(self)

    def update_overflow_data(self, new_overflow: bytes):
        """
        update overflow data, if it has dumped before, update the dumped data
        to avoid re-dump next time. Dump is time-consuming.
        """
        self.overflow_data = new_overflow
        if self._dumped:  # sync-updating dumped data
            header_len = NODE_TYPE_LENGTH_LIMIT + PAGE_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT
            if len(self.overflow_data) + header_len > self.tree_conf.page_size:  # overflow
                self.overflow_data = self._create_or_update_overflow(self.overflow_data, header_len)
                assert len(self.overflow_data) == self.tree_conf.page_size - header_len
            elif len(self.overflow_data) + header_len <= self.tree_conf.page_size and self.next_page:
                # overflow before, but normal currently
                self.tree.handler.get_node(self.next_page, tree=self.tree).set_as_deprecated()
                self.next_page = None
            of_len_start = NODE_TYPE_LENGTH_LIMIT
            of_len_end = of_len_start + PAGE_LENGTH_LIMIT
            self._dumped[of_len_start:of_len_end] = len(self.overflow_data).to_bytes(PAGE_LENGTH_LIMIT, ENDIAN)
            next_page = 0 if self.next_page is None else self.next_page
            self._dumped[of_len_end:header_len] = next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
            self._dumped[header_len:] = self.overflow_data
            if len(self._dumped) < self.tree_conf.page_size:
                padding = bytearray(self.tree_conf.page_size - len(self._dumped))
                self._dumped.extend(padding)
            assert len(self._dumped) == self.tree_conf.page_size

    def get_complete_data(self) -> bytes:
        """
        There may more than one overflow page, merge all overflow data and return it to parent,
        [BNode or OverflowNode]
        """
        if self.next_page:
            next_overflow = self.tree.handler.get_node(self.next_page, tree=self.tree)
            # assert isinstance(next_overflow, OverflowNode)
            return self.overflow_data + next_overflow.get_complete_data()
        else:
            return self.overflow_data

    def set_as_deprecated(self):
        """
        length of overflow data is now under page size, set pages after this in overflow-pages-chain
        as deprecated.
        """
        new_type_as_bytes = _PageType.DEPRECATED_PAGE.value.to_bytes(NODE_TYPE_LENGTH_LIMIT, ENDIAN)
        if self.next_page:
            self.tree.handler.get_node(self.next_page, tree=self.tree).set_as_deprecated()
            self.next_page = None
        self.tree.handler.set_deprecated_data(self.page, new_type_as_bytes)
        self.tree.handler.collect_deprecated_page(self.page)


class BNode(BaseBNode):
    __slots__ = ('tree', 'contents', 'children', 'tree_conf', 'page', 'next_page', 'overflow_data')
    PAGE_TYPE = _PageType.NORMAL_PAGE

    def __init__(self, tree, tree_conf: TreeConf, contents: list = None, children: list = None, page: int = None,
                 next_page: int = None, data: bytes = None):
        self.tree = tree
        self.tree_conf = tree_conf
        self.contents = contents or []
        self.children = children or []
        self.page = page or self.tree.next_available_page
        self.next_page = next_page
        if data:
            self.load(data)
        self._dumped = None  # internal dump-cache. Re-dump every time is extremely expensive.
        if self.children:
            assert len(self.contents) + 1 == len(self.children), \
                'One more child than overflow_data item required'

    def __repr__(self):
        name = 'Branch' if getattr(self, 'children', None) else 'Leaf'
        return '<{name} [pairs= {pairs}] [children= {children}]>'.format(
            name=name, pairs=','.join([str(it) for it in self.contents]),
            children=','.join([str(ch) for ch in self.children]))

    __str__ = __repr__

    def load(self, data: bytes):
        assert len(data) == self.tree_conf.page_size
        pairs_len_end = NODE_TYPE_LENGTH_LIMIT + NODE_CONTENTS_LENGTH_LIMIT
        pairs_len = int.from_bytes(data[NODE_TYPE_LENGTH_LIMIT:pairs_len_end], ENDIAN)
        children_len_end = pairs_len_end + NODE_CONTENTS_LENGTH_LIMIT
        children_len = int.from_bytes(data[pairs_len_end:children_len_end], ENDIAN)
        header_end = children_len_end + PAGE_ADDRESS_LIMIT
        self.next_page = int.from_bytes(data[children_len_end:header_end], ENDIAN)
        if self.next_page == 0:
            self.next_page = None
        else:
            overflow_node = self.tree.handler.get_node(self.next_page, tree=self.tree)
            # assert isinstance(overflow_node, OverflowNode)
            data += overflow_node.get_complete_data()
        each_pair_len = KeyValPair(self.tree_conf).length
        pairs_end = header_end + pairs_len
        for off_set in range(header_end, pairs_end, each_pair_len):
            pair = KeyValPair(self.tree_conf, data=data[off_set:(off_set + each_pair_len)])
            self.contents.append(pair)
        children_end = pairs_end + children_len
        assert children_end <= len(data)
        for off_set in range(pairs_end, children_end, PAGE_ADDRESS_LIMIT):
            self.children.append(int.from_bytes(data[off_set:(off_set + PAGE_ADDRESS_LIMIT)], ENDIAN))

    def _dump(self):
        data = bytearray()
        for pair in self.contents:
            data.extend(pair.dump())
        pairs_len = len(data)
        for ch in self.children:
            data.extend(ch.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN))
        children_len = len(data) - pairs_len
        header_len = NODE_TYPE_LENGTH_LIMIT + 2 * NODE_CONTENTS_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT

        self._adjust_overflow_chain(data, header_len)

        next_page = 0 if self.next_page is None else self.next_page
        header = (
                self.PAGE_TYPE.value.to_bytes(NODE_TYPE_LENGTH_LIMIT, ENDIAN) +
                pairs_len.to_bytes(NODE_CONTENTS_LENGTH_LIMIT, ENDIAN) +
                children_len.to_bytes(NODE_CONTENTS_LENGTH_LIMIT, ENDIAN) +
                next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
        )
        data = bytearray(header) + data
        if len(data) < self.tree_conf.page_size:
            padding = bytearray(self.tree_conf.page_size - len(data))
            data.extend(padding)
        self._dumped = data

    def dump(self) -> bytes:

        if self._dumped:
            return bytes(self._dumped)

        self._dump()
        return bytes(self._dumped)

    def re_dump(self):
        """
        Intent to update self._dumped forcibly, only call by BNode instances.
        """
        self._dump()

    def _adjust_overflow_chain(self, data: bytearray, header_len: int):
        if len(data) + header_len > self.tree_conf.page_size:  # overflow
            data[:] = self._create_or_update_overflow(bytes(data), header_len)
            assert len(data) == self.tree_conf.page_size - header_len
        elif len(data) + header_len <= self.tree_conf.page_size and self.next_page:
            # overflow before, but normal currently
            self.tree.handler.get_node(self.next_page, tree=self.tree).set_as_deprecated()
            self.next_page = None  # critical!

    # methods for sync data in internal dumped data with append/insert/update/pop ops
    # all methods must applied immediately when matched operation occurs.
    def update_content_in_dump(self, index: int, pair: KeyValPair):
        assert index < len(self.contents)
        if self._dumped:
            header_len = NODE_TYPE_LENGTH_LIMIT + 2 * NODE_CONTENTS_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT
            header = self._dumped[0:header_len]
            pairs_len_end = NODE_TYPE_LENGTH_LIMIT + NODE_CONTENTS_LENGTH_LIMIT
            pairs_len = int.from_bytes(bytes(header[NODE_TYPE_LENGTH_LIMIT:pairs_len_end]), ENDIAN)
            children_len = int.from_bytes(
                bytes(header[pairs_len_end:pairs_len_end + NODE_CONTENTS_LENGTH_LIMIT]), ENDIAN)
            orig = self._dumped[header_len:header_len + pairs_len + children_len]  # origin concrete data
            if self.next_page:
                orig += bytearray(self.tree.handler.get_node(self.next_page, tree=self.tree).get_complete_data())
            each_pair_len = KeyValPair(self.tree_conf).length
            target_start = each_pair_len * index
            orig[target_start:target_start + each_pair_len] = pair.dump()

            self._adjust_overflow_chain(orig, header_len)

            next_page = 0 if self.next_page is None else self.next_page
            header[-PAGE_ADDRESS_LIMIT:] = next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
            self._dumped = header + orig
            if len(self._dumped) < self.tree_conf.page_size:
                padding = bytearray(self.tree_conf.page_size - len(self._dumped))
                self._dumped.extend(padding)
            assert len(self._dumped) == self.tree_conf.page_size

    def insert_content_in_dump(self, index: int, pair: KeyValPair):
        assert index <= len(self.contents)
        if self._dumped:
            header_len = NODE_TYPE_LENGTH_LIMIT + 2 * NODE_CONTENTS_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT
            header = self._dumped[0:header_len]
            pairs_len_end = NODE_TYPE_LENGTH_LIMIT + NODE_CONTENTS_LENGTH_LIMIT
            pairs_len = int.from_bytes(bytes(header[NODE_TYPE_LENGTH_LIMIT:pairs_len_end]), ENDIAN)
            children_len = int.from_bytes(
                bytes(header[pairs_len_end:pairs_len_end + NODE_CONTENTS_LENGTH_LIMIT]), ENDIAN)
            orig = self._dumped[header_len:header_len + pairs_len + children_len]  # origin concrete data
            if self.next_page:
                orig += bytearray(self.tree.handler.get_node(self.next_page, tree=self.tree).get_complete_data())
            each_pair_len = KeyValPair(self.tree_conf).length
            target_start = each_pair_len * index
            orig[target_start:target_start] = pair.dump()  # insert at pos: target start
            pairs_len += each_pair_len  # update new pairs length
            # update new pairs length in header
            header[NODE_TYPE_LENGTH_LIMIT:pairs_len_end] = pairs_len.to_bytes(NODE_CONTENTS_LENGTH_LIMIT, ENDIAN)

            self._adjust_overflow_chain(orig, header_len)

            next_page = 0 if self.next_page is None else self.next_page
            header[-PAGE_ADDRESS_LIMIT:] = next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
            self._dumped = header + orig
            if len(self._dumped) < self.tree_conf.page_size:
                padding = bytearray(self.tree_conf.page_size - len(self._dumped))
                self._dumped.extend(padding)
            assert len(self._dumped) == self.tree_conf.page_size

    def pop_content_in_dump(self, index: int):
        assert index <= len(self.contents)
        if self._dumped:
            header_len = NODE_TYPE_LENGTH_LIMIT + 2 * NODE_CONTENTS_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT
            header = self._dumped[0:header_len]
            pairs_len_end = NODE_TYPE_LENGTH_LIMIT + NODE_CONTENTS_LENGTH_LIMIT
            pairs_len = int.from_bytes(bytes(header[NODE_TYPE_LENGTH_LIMIT:pairs_len_end]), ENDIAN)
            children_len = int.from_bytes(
                bytes(header[pairs_len_end:pairs_len_end + NODE_CONTENTS_LENGTH_LIMIT]), ENDIAN)
            orig = self._dumped[header_len:header_len + pairs_len + children_len]  # origin concrete data
            if self.next_page:
                orig += bytearray(self.tree.handler.get_node(self.next_page, tree=self.tree).get_complete_data())
            each_pair_len = KeyValPair(self.tree_conf).length
            target_start = each_pair_len * index
            # remove target pair in dumped data at pos: target start
            orig[target_start:target_start + each_pair_len] = b''
            pairs_len -= each_pair_len  # update new pairs length
            # update new pairs length in header
            header[NODE_TYPE_LENGTH_LIMIT:pairs_len_end] = pairs_len.to_bytes(NODE_CONTENTS_LENGTH_LIMIT, ENDIAN)

            self._adjust_overflow_chain(orig, header_len)

            next_page = 0 if self.next_page is None else self.next_page
            header[-PAGE_ADDRESS_LIMIT:] = next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
            self._dumped = header + orig
            if len(self._dumped) < self.tree_conf.page_size:
                padding = bytearray(self.tree_conf.page_size - len(self._dumped))
                self._dumped.extend(padding)
            assert len(self._dumped) == self.tree_conf.page_size

    def update_child_in_dump(self, index: int, child: int):
        assert index < len(self.children)
        if self._dumped:
            header_len = NODE_TYPE_LENGTH_LIMIT + 2 * NODE_CONTENTS_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT
            header = self._dumped[0:header_len]
            pairs_len_end = NODE_TYPE_LENGTH_LIMIT + NODE_CONTENTS_LENGTH_LIMIT
            pairs_len = int.from_bytes(bytes(header[NODE_TYPE_LENGTH_LIMIT:pairs_len_end]), ENDIAN)
            children_len = int.from_bytes(
                bytes(header[pairs_len_end:pairs_len_end + NODE_CONTENTS_LENGTH_LIMIT]), ENDIAN)
            orig = self._dumped[header_len:header_len + pairs_len + children_len]  # origin concrete
            if self.next_page:
                orig += bytearray(self.tree.handler.get_node(self.next_page, tree=self.tree).get_complete_data())
            target_start = pairs_len + index * PAGE_ADDRESS_LIMIT
            orig[target_start:target_start + PAGE_ADDRESS_LIMIT] = child.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)

            self._adjust_overflow_chain(orig, header_len)

            next_page = 0 if self.next_page is None else self.next_page
            header[-PAGE_ADDRESS_LIMIT:] = next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
            self._dumped = header + orig
            if len(self._dumped) < self.tree_conf.page_size:
                padding = bytearray(self.tree_conf.page_size - len(self._dumped))
                self._dumped.extend(padding)
            assert len(self._dumped) == self.tree_conf.page_size

    def insert_child_in_dump(self, index: int, child: int):
        assert index <= len(self.children)
        if self._dumped:
            header_len = NODE_TYPE_LENGTH_LIMIT + 2 * NODE_CONTENTS_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT
            header = self._dumped[0:header_len]
            pairs_len_end = NODE_TYPE_LENGTH_LIMIT + NODE_CONTENTS_LENGTH_LIMIT
            pairs_len = int.from_bytes(bytes(header[NODE_TYPE_LENGTH_LIMIT:pairs_len_end]), ENDIAN)
            children_len = int.from_bytes(
                bytes(header[pairs_len_end:pairs_len_end + NODE_CONTENTS_LENGTH_LIMIT]), ENDIAN)
            orig = self._dumped[header_len:header_len + pairs_len + children_len]  # origin concrete data
            if self.next_page:
                orig += bytearray(self.tree.handler.get_node(self.next_page, tree=self.tree).get_complete_data())
            target_start = pairs_len + index * PAGE_ADDRESS_LIMIT
            orig[target_start:target_start] = child.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)  # insert at pos: target start
            children_len += PAGE_ADDRESS_LIMIT  # update new pairs length
            # update new children length in header
            header[pairs_len_end:pairs_len_end + NODE_CONTENTS_LENGTH_LIMIT] = children_len.to_bytes(
                NODE_CONTENTS_LENGTH_LIMIT, ENDIAN)

            self._adjust_overflow_chain(orig, header_len)

            next_page = 0 if self.next_page is None else self.next_page
            header[-PAGE_ADDRESS_LIMIT:] = next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
            self._dumped = header + orig
            if len(self._dumped) < self.tree_conf.page_size:
                padding = bytearray(self.tree_conf.page_size - len(self._dumped))
                self._dumped.extend(padding)
            assert len(self._dumped) == self.tree_conf.page_size

    def pop_child_in_dump(self, index: int):
        assert index <= len(self.children)
        if self._dumped:
            header_len = NODE_TYPE_LENGTH_LIMIT + 2 * NODE_CONTENTS_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT
            header = self._dumped[0:header_len]
            pairs_len_end = NODE_TYPE_LENGTH_LIMIT + NODE_CONTENTS_LENGTH_LIMIT
            pairs_len = int.from_bytes(bytes(header[NODE_TYPE_LENGTH_LIMIT:pairs_len_end]), ENDIAN)
            children_len = int.from_bytes(
                bytes(header[pairs_len_end:pairs_len_end + NODE_CONTENTS_LENGTH_LIMIT]), ENDIAN)
            orig = self._dumped[header_len:header_len + pairs_len + children_len]  # origin concrete data
            if self.next_page:
                orig += bytearray(self.tree.handler.get_node(self.next_page, tree=self.tree).get_complete_data())
            target_start = pairs_len + index * PAGE_ADDRESS_LIMIT
            orig[target_start:target_start + PAGE_ADDRESS_LIMIT] = b''  # remove at pos: target start
            children_len -= PAGE_ADDRESS_LIMIT  # update new pairs length
            # update new children length in header
            header[pairs_len_end:pairs_len_end + NODE_CONTENTS_LENGTH_LIMIT] = children_len.to_bytes(
                NODE_CONTENTS_LENGTH_LIMIT, ENDIAN)

            self._adjust_overflow_chain(orig, header_len)

            next_page = 0 if self.next_page is None else self.next_page
            header[-PAGE_ADDRESS_LIMIT:] = next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
            self._dumped = header + orig
            if len(self._dumped) < self.tree_conf.page_size:
                padding = bytearray(self.tree_conf.page_size - len(self._dumped))
                self._dumped.extend(padding)
            assert len(self._dumped) == self.tree_conf.page_size

    def lateral(self, parent, parent_index, target, target_index):
        """
        lend one element from parent[parent_index] to target[target_index].
        """
        if parent_index > target_index:
            target.contents.append(parent.contents[target_index])
            # origin len(target.content) == current len(...) - 1, cuz append already
            target.insert_content_in_dump(len(target.contents) - 1, parent.contents[target_index])
            content_to_pop = self.contents.pop(0)
            self.pop_content_in_dump(0)
            parent.contents[target_index] = content_to_pop
            parent.update_content_in_dump(target_index, content_to_pop)
            if self.children:
                child_to_pop = self.children.pop(0)
                self.pop_child_in_dump(0)
                target.children.append(child_to_pop)
                target.insert_child_in_dump(len(target.children) - 1, child_to_pop)
        else:
            target.contents.insert(0, parent.contents[parent_index])
            target.insert_content_in_dump(0, parent.contents[parent_index])
            content_to_pop = self.contents.pop()
            # origin tail index == current len(...), cuz pop already
            self.pop_content_in_dump(len(self.contents))
            parent.contents[parent_index] = content_to_pop
            parent.update_content_in_dump(parent_index, content_to_pop)
            if self.children:
                child_to_pop = self.children.pop()
                self.pop_child_in_dump(len(self.children))
                target.children.insert(0, child_to_pop)
                target.insert_child_in_dump(0, child_to_pop)
        # update nodes inside handler
        self.tree.handler.set_node(parent)
        self.tree.handler.set_node(target)

    def shrink(self, ancestors: list):
        """
        shrink from current node up to the root until test_tree is balanced.
        :param ancestors: ancestors from root to current node
        """
        parent = None

        if ancestors:
            parent, parent_index = ancestors.pop()
            # try to lend to the left neighboring sibling
            if parent_index:
                left_sib = self.tree.handler.get_node(parent.children[parent_index - 1], tree=self.tree)
                if len(left_sib.contents) < self.tree_conf.order:
                    self.lateral(
                        parent, parent_index, left_sib, parent_index - 1)
                    return

            # try the right neighbor
            if parent_index + 1 < len(parent.children):
                right_sib = self.tree.handler.get_node(parent.children[parent_index + 1], tree=self.tree)
                if len(right_sib.contents) < self.tree_conf.order:
                    self.lateral(
                        parent, parent_index, right_sib, parent_index + 1)
                    return

        sibling, mid_pair = self.split()

        if not parent:
            parent, parent_index = self.tree.BRANCH(tree=self.tree,
                                                    tree_conf=self.tree_conf, children=[self.page]), 0
            self.tree._root = parent
            self.tree.handler.ensure_root_block(self.tree._root)

        # pass the median up to the parent
        parent.contents.insert(parent_index, mid_pair)
        parent.insert_content_in_dump(parent_index, mid_pair)
        parent.children.insert(parent_index + 1, sibling.page)
        parent.insert_child_in_dump(parent_index + 1, sibling.page)
        if len(parent.contents) > parent.tree.order:
            parent.shrink(ancestors)
        self.tree.handler.set_node(parent)  # sync
        self.tree.handler.set_node(sibling)  # IMPORTANT!

    def split(self):
        """
        split this node into two parts
        :returns: new node and median elements
        """
        center = len(self.contents) // 2
        mid_pair = self.contents[center]
        sibling = type(self)(
            tree=self.tree,
            tree_conf=self.tree_conf,
            contents=self.contents[center + 1:],
            children=self.children[center + 1:])
        self.contents = self.contents[:center]
        self.children = self.children[:center + 1]
        self._dump()  # update self._dumped
        return sibling, mid_pair

    def grow(self, ancestors: list):
        """
        grow from current node up to the root until test_tree is balanced,
        by trying borrowing items from siblings or consolidate with siblings.
        :param ancestors: ancestors from root to current node
        """
        parent, parent_index = ancestors.pop()
        left_sib = right_sib = None
        # try to borrow from the right sibling
        if parent_index + 1 < len(parent.children):
            right_sib = self.tree.handler.get_node(parent.children[parent_index + 1], tree=self.tree)
            if len(right_sib.contents) > self.tree.min_elements:
                right_sib.lateral(parent, parent_index + 1, self, parent_index)
                return

        # try to borrow from the left sibling
        if parent_index:
            left_sib = self.tree.handler.get_node(parent.children[parent_index - 1], tree=self.tree)
            if len(left_sib.contents) > self.tree.min_elements:
                left_sib.lateral(parent, parent_index - 1, self, parent_index)
                return

        # consolidate with a sibling - try left first
        if left_sib:
            left_sib.contents.append(parent.contents[parent_index - 1])
            left_sib.contents.extend(self.contents)
            if self.children:
                left_sib.children.extend(self.children)
            left_sib.re_dump()
            parent.contents.pop(parent_index - 1)
            parent.children.pop(parent_index)
            parent.pop_content_in_dump(parent_index - 1)
            parent.pop_child_in_dump(parent_index)
            # sync
            self.tree.handler.set_node(left_sib)
            self.tree.handler.set_node(parent)
        else:
            self.contents.append(parent.contents[parent_index])
            self.contents.extend(right_sib.contents)
            if self.children:
                self.children.extend(right_sib.children)
            self.re_dump()
            parent.contents.pop(parent_index)
            parent.children.pop(parent_index + 1)
            parent.pop_content_in_dump(parent_index)
            parent.pop_child_in_dump(parent_index + 1)
            # sync
            self.tree.handler.set_node(self)
            self.tree.handler.set_node(parent)

        if len(parent.contents) < self.tree.min_elements:
            if ancestors:
                # parent is not the root
                parent.grow(ancestors)
            elif not parent.contents:
                # parent is root, and it's now empty
                self.tree._root = left_sib or self
                self.tree.handler.ensure_root_block(self.tree._root)

    def insert(self, index, key, value, ancestors):
        pair_to_insert = KeyValPair(self.tree_conf, key=key, value=value)
        self.contents.insert(index, pair_to_insert)
        self.insert_content_in_dump(index, pair_to_insert)
        if len(self.contents) > self.tree_conf.order:
            self.shrink(ancestors)
        self.tree.handler.set_node(self)

    def remove(self, index, ancestors):

        if self.children:
            # try promoting from the right subtree first,
            # but only if it won't have to resize
            additional_ancestors = [(self, index + 1)]
            descendant = self.tree.handler.get_node(self.children[index + 1], tree=self.tree)
            while descendant.children:
                additional_ancestors.append((descendant, 0))
                descendant = self.tree.handler.get_node(descendant.children[0], tree=self.tree)
            if len(descendant.contents) > self.tree.min_elements:
                ancestors.extend(additional_ancestors)
                self.contents[index] = descendant.contents[0]
                self.update_content_in_dump(index, descendant.contents[0])
                descendant.remove(0, ancestors)
                self.tree.handler.set_node(self)
                self.tree.handler.set_node(descendant)
                return

            # fall back to the left child
            additional_ancestors = [(self, index)]
            descendant = self.tree.handler.get_node(self.children[index], tree=self.tree)
            while descendant.children:
                additional_ancestors.append(
                    (descendant, len(descendant.children) - 1))
                descendant = self.tree.handler.get_node(descendant.children[-1], tree=self.tree)
            ancestors.extend(additional_ancestors)
            self.contents[index] = descendant.contents[-1]
            self.update_content_in_dump(index, descendant.contents[-1])
            descendant.remove(len(descendant.children) - 1, ancestors)
            self.tree.handler.set_node(self)
            self.tree.handler.set_node(descendant)
        else:
            self.contents.pop(index)
            self.pop_content_in_dump(index)
            if len(self.contents) < self.tree.min_elements and ancestors:
                self.grow(ancestors)
            self.tree.handler.set_node(self)
