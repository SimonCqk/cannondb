import enum
import struct
from abc import ABCMeta, abstractmethod

from cannondb.const import (ENDIAN, NODE_CONTENTS_SIZE_LIMIT, PAGE_ADDRESS_LIMIT, PAGE_LENGTH_LIMIT, KEY_LENGTH_LIMIT,
                            KEY_LENGTH_FORMAT,
                            VALUE_LENGTH_FORMAT, VALUE_LENGTH_LIMIT, NODE_TYPE_LENGTH_LIMIT, TreeConf)


class KeyValPair(metaclass=ABCMeta):
    __slots__ = ('key', 'value', 'length', 'tree_conf')

    def __init__(self, tree_conf: TreeConf, key=None, value=None, data: bytes = None):
        self.tree_conf = tree_conf
        self.key = key
        if key:
            assert len(key) <= self.tree_conf.key_size
        if value:
            assert len(value) <= self.tree_conf.value_size
        self.value = value
        self.length = (KEY_LENGTH_LIMIT + self.tree_conf.key_size
                       + VALUE_LENGTH_LIMIT + self.tree_conf.value_size)
        if data:
            self.load(data)

    def load(self, data: bytes):
        assert len(data) == self.length
        key_len = struct.unpack(KEY_LENGTH_FORMAT, data[0:KEY_LENGTH_LIMIT])[0]

        assert 0 <= key_len <= self.tree_conf.key_size

        key_end = KEY_LENGTH_LIMIT + key_len
        self.key = data[key_len:key_end].decode('utf-8')

        val_len_start = key_end + self.tree_conf.key_size
        val_len_end = val_len_start + VALUE_LENGTH_LIMIT
        val_len = struct.unpack(VALUE_LENGTH_FORMAT, data[val_len_start:val_len_end])[0]

        assert 0 <= val_len <= self.tree_conf.value_size
        val_end = val_len_end + val_len
        self.value = self.tree_conf.serializer.deserialize(data[val_len_end:val_end])

    def dump(self) -> bytes:
        assert self.key and self.value
        key_as_bytes = self.tree_conf.serializer.serialize(self.key)
        key_len = len(key_as_bytes)
        val_as_bytes = self.tree_conf.serializer.serialize(self.value)
        val_len = len(val_as_bytes)
        data = (
                struct.pack(KEY_LENGTH_FORMAT, key_len) +
                key_as_bytes +
                bytes(self.tree_conf.key_size - key_len) +
                struct.pack(VALUE_LENGTH_FORMAT, val_len) +
                val_as_bytes +
                bytes(self.tree_conf.value_size - val_len)
        )
        return data

    def __eq__(self, other):
        return self.key == other.key

    def __lt__(self, other):
        return self.key < other.key

    def __le__(self, other):
        return self.key <= other.key

    def __gt__(self, other):
        return self.key > other.key

    def __ge__(self, other):
        return self.key >= other.key

    def __str__(self):
        return '<{key}:{val}>'.format(key=self.key, val=self.value)

    __repr__ = __str__


class _NodeType(enum.Enum):
    NORMAL_NODE = 0
    OVERFLOW_NODE = 1


class BaseBNode(metaclass=ABCMeta):
    __slots__ = ()
    NODE_TYPE = None

    @abstractmethod
    def load(self, data: bytes):
        """create node from raw data"""
        pass

    @abstractmethod
    def dump(self) -> bytes:
        """convert node to bytes which contains all information of this node"""
        pass

    @classmethod
    def from_raw_data(cls, tree_conf: TreeConf, page: int, data: bytes):
        node_type = int.from_bytes(data[0:NODE_TYPE_LENGTH_LIMIT], ENDIAN)
        if node_type == 0:
            return BNode(tree_conf, page=page, data=data)
        elif node_type == 1:
            return OverflowNode(tree_conf, page=page, data=data)
        else:
            raise TypeError('No such node type:{type} matched'.format(type=node_type))


class OverflowNode(BaseBNode):
    """Recording overflow pages' information and raw data"""
    __slots__ = ('tree_conf', 'page', 'parent_page', 'next_page', 'data')
    NODE_TYPE = _NodeType.OVERFLOW_NODE

    def __init__(self, tree_conf: TreeConf, page: int, parent_page: int = None, next_page: int = None,
                 data: bytes = None):
        self.tree_conf = tree_conf
        self.page = page
        self.parent_page = parent_page
        self.next_page = next_page
        self.data = data
        if data:
            self.load(data)

    def load(self, data: bytes):
        data_len_end = NODE_TYPE_LENGTH_LIMIT + PAGE_LENGTH_LIMIT
        data_len = int.from_bytes(data[NODE_TYPE_LENGTH_LIMIT:data_len_end], ENDIAN)
        header_end = data_len_end + PAGE_ADDRESS_LIMIT
        self.next_page = int.from_bytes(data[data_len_end:header_end], ENDIAN)
        if self.next_page == 0:
            self.next_page = None
        self.data = data[header_end:data_len]

    def dump(self) -> bytes:
        header_len = NODE_TYPE_LENGTH_LIMIT + PAGE_LENGTH_LIMIT + PAGE_ADDRESS_LIMIT
        if len(self.data) + header_len > self.tree_conf.page_size:
            detach_start = self.tree_conf.page_size - header_len
            self.next_page = self.tree_conf.tree.next_available_page()
            of = OverflowNode(self.tree_conf, self.next_page, self.page, data=self.data[detach_start:])
            of.flush()
            self.data = self.data[0:detach_start]
        header = (
                self.NODE_TYPE.value.to_bytes(NODE_TYPE_LENGTH_LIMIT, ENDIAN) +
                len(self.data).to_bytes(PAGE_LENGTH_LIMIT, ENDIAN) +
                self.next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
        )
        padding = self.tree_conf.page_size - header_len - len(self.data)
        assert 0 <= padding
        return header + self.data + bytes(padding)

    def flush(self):
        if self.data:
            pass


class BNode(BaseBNode):
    __slots__ = ('contents', 'children', 'tree_conf', 'page', 'next_page', 'data')
    NODE_TYPE = _NodeType.NORMAL_NODE

    def __init__(self, tree_conf: TreeConf, contents: list = None, children: list = None, page: int = None,
                 next_page: int = None, data: bytes = None):
        self.tree_conf = tree_conf
        self.contents = contents or []
        self.children = children or []
        self.page = page
        self.next_page = next_page
        if data:
            self.load(data)
        if self.children:
            assert len(self.contents) + 1 == len(self.children), \
                'One more child than data item required'

    def __repr__(self):
        name = 'Branch' if getattr(self, 'children', None) else 'Leaf'
        return '<{name} [{pairs}]>'.format(
            name=name, pairs=','.join([str(it) for it in self.contents]))

    def load(self, data: bytes):
        assert len(data) == self.tree_conf.page_size
        pairs_len_end = NODE_TYPE_LENGTH_LIMIT + NODE_CONTENTS_SIZE_LIMIT
        pairs_len = int.from_bytes(data[NODE_TYPE_LENGTH_LIMIT:pairs_len_end], ENDIAN)
        children_len_end = pairs_len_end + NODE_CONTENTS_SIZE_LIMIT
        children_len = int.from_bytes(data[pairs_len_end:children_len_end], ENDIAN)
        header_end = children_len_end + PAGE_ADDRESS_LIMIT
        self.next_page = int.from_bytes(data[children_len_end:header_end], ENDIAN)
        if self.next_page == 0:
            self.next_page = None
        each_pair_len = KeyValPair(self.tree_conf).length
        pairs_end = header_end + pairs_len
        for off_set in range(header_end, pairs_end, each_pair_len):
            pair = KeyValPair(self.tree_conf, data=data[off_set:(off_set + each_pair_len)])
            self.contents.append(pair)
        children_end = pairs_end + children_len
        assert children_end <= len(data)
        for off_set in range(pairs_end, children_end, PAGE_ADDRESS_LIMIT):
            self.children.append(int.from_bytes(data[off_set:(off_set + PAGE_ADDRESS_LIMIT)], ENDIAN))

    def dump(self) -> bytes:
        data = bytearray()
        for pair in self.contents:
            data.extend(pair.dump())
        pairs_len = len(data)
        for ch in self.children:
            data.extend(ch.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN))
        children_len = len(data) - pairs_len

        header_len = NODE_TYPE_LENGTH_LIMIT + 2 * NODE_CONTENTS_SIZE_LIMIT + PAGE_ADDRESS_LIMIT
        if len(data) + header_len > self.tree_conf.page_size:  # overflow
            detach_start = self.tree_conf.page_size - header_len
            self.next_page = self.tree_conf.tree.next_available_page()
            of = OverflowNode(self.tree_conf, self.next_page, self.page, data=bytes(data[detach_start:]))
            of.flush()
            data = data[0:detach_start]
        next_page = 0 if self.next_page is None else self.next_page
        header = (
                self.NODE_TYPE.value.to_bytes(NODE_TYPE_LENGTH_LIMIT, ENDIAN) +
                pairs_len.to_bytes(NODE_CONTENTS_SIZE_LIMIT, ENDIAN) +
                children_len.to_bytes(NODE_CONTENTS_SIZE_LIMIT, ENDIAN) +
                next_page.to_bytes(PAGE_ADDRESS_LIMIT, ENDIAN)
        )
        data = bytearray(header) + data
        if len(data) < self.tree_conf.page_size:
            padding = bytearray(self.tree_conf.page_size - len(data))
            data.extend(padding)
        return bytes(data)

    def lateral(self, parent, parent_index, target, target_index):
        """
        lend one element from parent[parent_index] to target[target_index].
        """
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
                if len(left_sib.contents) < self.tree_conf.tree.order:
                    self.lateral(
                        parent, parent_index, left_sib, parent_index - 1)
                    return

            # try the right neighbor
            if parent_index + 1 < len(parent.children):
                right_sib = parent.children[parent_index + 1]
                if len(right_sib.contents) < self.tree_conf.tree.order:
                    self.lateral(
                        parent, parent_index, right_sib, parent_index + 1)
                    return

        sibling, mid_pair = self.split()

        if not parent:
            parent, parent_index = self.tree_conf.tree.BRANCH(
                self.tree_conf, children=[self]), 0
            self.tree_conf.tree._root = parent

        # pass the median up to the parent
        parent.contents.insert(parent_index, mid_pair)
        parent.children.insert(parent_index + 1, sibling)
        if len(parent.contents) > parent.tree.order:
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
            if len(right_sib.contents) > self.tree_conf.tree.min_elements:
                right_sib.lateral(parent, parent_index + 1, self, parent_index)
                return

        # try to borrow from the left sibling
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len(left_sib.contents) > self.tree_conf.tree.min_elements:
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

        if len(parent.contents) < self.tree_conf.tree.min_elements:
            if ancestors:
                # parent is not the root
                parent.grow(ancestors)
            elif not parent.contents:
                # parent is root, and it's now empty
                self.tree_conf.tree._root = left_sib or self

    def split(self):
        """
        split this node into two parts
        :returns: new node and median elements
        """
        center = len(self.contents) // 2
        mid_pair = self.contents[center]
        sibling = type(self)(
            self.tree_conf,
            self.contents[center + 1:],
            self.children[center + 1:])
        self.contents = self.contents[:center]
        self.children = self.children[:center + 1]
        return sibling, mid_pair

    def insert(self, index, key, value, ancestors):
        self.contents.insert(index, KeyValPair(self.tree_conf, key, value))
        if len(self.contents) > self.tree_conf.tree.order:
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
            if len(descendant.contents) > self.tree_conf.tree.min_elements:
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
            if len(self.contents) < self.tree_conf.tree.min_elements and ancestors:
                self.grow(ancestors)
