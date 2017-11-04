'''
This file include the specific implementation of B+ tree.

'''
import bisect
import operator


class _BPlusBranch( object ):
    __slots__ = ["tree", "contents", "children"]

    def __init__(self, tree, contents=None, children=None):
        self.tree = tree
        self.contents = contents or []
        self.children = children or []
        if self.children:
            assert len( self.contents ) + 1 == len( self.children ), \
                "one more child than data item required"

    def __repr__(self):
        name = getattr( self, "children", 0 ) and "Branch" or "Leaf"
        return '<{name} {contents}>'.format(
            name=name, contents=', '.join( [str( i ) for i in self.contents] ) )

    def split(self):
        center = len( self.contents ) // 2
        median = self.contents[center]
        sibling = type( self )(
            self.tree,
            self.contents[center + 1:],
            self.children[center + 1:] )
        self.contents = self.contents[:center]
        self.children = self.children[:center + 1]
        return sibling, median

    def grow(self, ancestors):
        parent, parent_index = ancestors.pop()

        minimum = self.tree.order // 2
        left_sib = right_sib = None

        # try to borrow from the right sibling
        if parent_index + 1 < len( parent.children ):
            right_sib = parent.children[parent_index + 1]
            if len( right_sib.contents ) > minimum:
                right_sib.lateral( parent, parent_index + 1, self, parent_index )
                return

        # try to borrow from the left sibling
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len( left_sib.contents ) > minimum:
                left_sib.lateral( parent, parent_index - 1, self, parent_index )
                return

        # consolidate with a sibling - try left first
        if left_sib:
            left_sib.contents.append( parent.contents[parent_index - 1] )
            left_sib.contents.extend( self.contents )
            if self.children:
                left_sib.children.extend( self.children )
            parent.contents.pop( parent_index - 1 )
            parent.children.pop( parent_index )
        else:
            self.contents.append( parent.contents[parent_index] )
            self.contents.extend( right_sib.contents )
            if self.children:
                self.children.extend( right_sib.children )
            parent.contents.pop( parent_index )
            parent.children.pop( parent_index + 1 )

        if len( parent.contents ) < minimum:
            if ancestors:
                # parent is not the root
                parent.grow( ancestors )
            elif not parent.contents:
                # parent is root, and its now empty
                self.tree._root = left_sib or self

    def shrink(self, ancestors):
        parent = None

        if ancestors:
            parent, parent_index = ancestors.pop()
            # try to lend to the left neighboring sibling
            if parent_index:
                left_sib = parent.children[parent_index - 1]
                if len( left_sib.contents ) < self.tree.order:
                    self.lateral(
                        parent, parent_index, left_sib, parent_index - 1 )
                    return

            # try the right neighbor
            if parent_index + 1 < len( parent.children ):
                right_sib = parent.children[parent_index + 1]
                if len( right_sib.contents ) < self.tree.order:
                    self.lateral(
                        parent, parent_index, right_sib, parent_index + 1 )
                    return

        sibling, push = self.split()

        if not parent:
            parent, parent_index = self.tree.BRANCH(
                self.tree, children=[self] ), 0
            self.tree._root = parent

        # pass the median up to the parent
        parent.contents.insert( parent_index, push )
        parent.children.insert( parent_index + 1, sibling )
        if len( parent.contents ) > parent.tree.order:
            parent.shrink( ancestors )

    def lateral(self, parent, parent_index, dest, dest_index):
        if parent_index > dest_index:
            dest.contents.append( parent.contents[dest_index] )
            parent.contents[dest_index] = self.contents.pop( 0 )
            if self.children:
                dest.children.append( self.children.pop( 0 ) )
        else:
            dest.contents.insert( 0, parent.contents[parent_index] )
            parent.contents[parent_index] = self.contents.pop()
            if self.children:
                dest.children.insert( 0, self.children.pop() )

    def insert(self, index, item, ancestors):
        self.contents.insert( index, item )
        if len( self.contents ) > self.tree.order:
            self.shrink( ancestors )

    def remove(self, index, ancestors):
        minimum = self.tree.order // 2

        if self.children:
            # try promoting from the right subtree first,
            # but only if it won't have to resize
            additional_ancestors = [(self, index + 1)]
            descendent = self.children[index + 1]
            while descendent.children:
                additional_ancestors.append( (descendent, 0) )
                descendent = descendent.children[0]
            if len( descendent.contents ) > minimum:
                ancestors.extend( additional_ancestors )
                self.contents[index] = descendent.contents[0]
                descendent.remove( 0, ancestors )
                return

            # fall back to the left child
            additional_ancestors = [(self, index)]
            descendent = self.children[index]
            while descendent.children:
                additional_ancestors.append(
                    (descendent, len( descendent.children ) - 1) )
                descendent = descendent.children[-1]
            ancestors.extend( additional_ancestors )
            self.contents[index] = descendent.contents[-1]
            descendent.remove( len( descendent.children ) - 1, ancestors )
        else:
            self.contents.pop( index )
            if len( self.contents ) < minimum and ancestors:
                self.grow( ancestors )


class _BPlusLeaf( object ):
    __slots__ = ["tree", "contents", "data", "next"]

    def __init__(self, tree, contents=None, data=None, next=None):
        self.tree = tree
        self.contents = contents or []
        self.data = data or []
        self.next = next  # 指向兄弟叶节点
        assert len( self.contents ) == len( self.data ), "one data per key"

    def shrink(self, ancestors):
        parent = None

        if ancestors:
            parent, parent_index = ancestors.pop()
            # try to lend to the left neighboring sibling
            if parent_index:
                left_sib = parent.children[parent_index - 1]
                if len( left_sib.contents ) < self.tree.order:
                    self.lateral(
                        parent, parent_index, left_sib, parent_index - 1 )
                    return

            # try the right neighbor
            if parent_index + 1 < len( parent.children ):
                right_sib = parent.children[parent_index + 1]
                if len( right_sib.contents ) < self.tree.order:
                    self.lateral(
                        parent, parent_index, right_sib, parent_index + 1 )
                    return

        sibling, push = self.split()

        if not parent:
            parent, parent_index = self.tree.BRANCH(
                self.tree, children=[self] ), 0
            self.tree._root = parent

        # pass the median up to the parent
        parent.contents.insert( parent_index, push )
        parent.children.insert( parent_index + 1, sibling )
        if len( parent.contents ) > parent.tree.order:
            parent.shrink( ancestors )

    def lateral(self, parent, parent_index, dest, dest_index):
        if parent_index > dest_index:
            dest.contents.append( self.contents.pop( 0 ) )
            dest.data.append( self.data.pop( 0 ) )
            parent.contents[dest_index] = self.contents[0]
        else:
            dest.contents.insert( 0, self.contents.pop() )
            dest.data.insert( 0, self.data.pop() )
            parent.contents[parent_index] = dest.contents[0]

    def split(self):  # 分裂成左右两部分
        center = len( self.contents ) // 2
        sibling = type( self )(  # 可以用staticmethod替换
            self.tree,
            self.contents[center:],
            self.data[center:],
            self.next )
        self.contents = self.contents[:center]
        self.data = self.data[:center]
        self.next = sibling
        return sibling, sibling.contents[0]

    def grow(self, ancestors):
        minimum = self.tree.order // 2
        parent, parent_index = ancestors.pop()
        left_sib = right_sib = None

        # try borrowing from a neighbor - try right first
        if parent_index + 1 < len( parent.children ):
            right_sib = parent.children[parent_index + 1]
            if len( right_sib.contents ) > minimum:
                right_sib.lateral( parent, parent_index + 1, self, parent_index )
                return

        # fallback to left
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len( left_sib.contents ) > minimum:
                left_sib.lateral( parent, parent_index - 1, self, parent_index )
                return

        # join with a neighbor - try left first
        if left_sib:
            left_sib.contents.extend( self.contents )
            left_sib.data.extend( self.data )
            parent.remove( parent_index - 1, ancestors )
            return

        # fallback to right
        self.contents.extend( right_sib.contents )
        self.data.extend( right_sib.data )
        parent.remove( parent_index, ancestors )

    def remove(self, index, ancestors):  # 与bnode不一样
        minimum = self.tree.order // 2  # 节点内不破坏B+树规则的的最小数目
        if index >= len( self.contents ):
            self, index = self.next, 0

        key = self.contents[index]

        # if any leaf that could accept the key can do so
        # without any rebalancing necessary, then go that route
        current = self
        while current is not None and current.contents[0] == key:
            if len( current.contents ) > minimum:
                if current.contents[0] == key:
                    index = 0
                else:
                    index = bisect.bisect_left( current.contents, key )
                current.contents.pop( index )
                current.data.pop( index )
                return
            current = current.next

        self.grow( ancestors )

    def insert(self, index, key, data, ancestors):  # 与bnode不一样
        self.contents.insert( index, key )
        self.data.insert( index, data )

        if len( self.contents ) > self.tree.order:
            self.shrink( ancestors )


class BPlusTree( object ):
    LEAF = _BPlusLeaf
    BRANCH = _BPlusBranch

    def __init__(self, order=100):
        self.order = order
        self._root = self._bottom = self.LEAF( self )

    def _get(self, key):
        node, index = self._path_to( key )[-1]

        if index == len( node.contents ):
            if node.next:
                node, index = node.next, 0
            else:
                yield

        while node.contents[index] == key:
            yield node.data[index]
            index += 1
            if index == len( node.contents ):
                if node.next:
                    node, index = node.next, 0
                else:
                    yield

    def _path_to_branch(self, item):
        current = self._root
        ancestry = []

        while getattr( current, "children", None ):
            index = bisect.bisect_left( current.contents, item )
            ancestry.append( (current, index) )
            if index < len( current.contents ) \
                    and current.contents[index] == item:
                return ancestry
            current = current.children[index]

        index = bisect.bisect_left( current.contents, item )
        ancestry.append( (current, index) )

        return ancestry

    def _path_to(self, item):  # 根节点到当前节点的路径
        path = self._path_to_branch( item )
        node, index = path[-1]
        while hasattr( node, "children" ):
            node = node.children[index]
            index = bisect.bisect_left( node.contents, item )
            path.append( (node, index) )
        return path

    def get(self, key, default=None):
        try:
            return self._get( key ).next()
        except StopIteration:
            return default

    def getlist(self, key):
        return list( self._get( key ) )

    def insert(self, key, data):
        path = self._path_to( key )
        node, index = path.pop()
        node.insert( index, key, data, path )

    def remove(self, key):
        path = self._path_to( key )
        node, index = path.pop()
        node.remove( index, path )

    __getitem__ = get
    __setitem__ = insert
    __delitem__ = remove

    def __contains__(self, key):
        for _ in self._get( key ):
            return True
        return False

    def __repr__(self):
        def recurse(node, accum, depth):
            accum.append( ("  " * depth) + repr( node ) )
            for node in getattr( node, 'children', [] ):
                recurse( node, accum, depth + 1 )

        accum = []
        recurse( self._root, accum, 0 )
        return "\n".join( accum )

    def iteritems(self):
        node = self._root
        while hasattr( node, "children" ):
            node = node.children[0]

        while node:
            for pair in zip( node.contents, node.data ):
                yield pair
            node = node.next

    def iterkeys(self):
        return [operator.itemgetter( 0 )( i ) for i in self.iteritems()]

    def itervalues(self):
        return [operator.itemgetter( 1 )( i ) for i in self.iteritems()]

    __iter__ = iterkeys

    def items(self):
        return list( self.iteritems() )

    def keys(self):
        return list( self.iterkeys() )

    def values(self):
        return list( self.itervalues() )

    def _build_bulk_loaded_leaves(self, items):
        minimum = self.order // 2
        leaves, seps = [[]], []

        for item in items:
            if len( leaves[-1] ) >= self.order:
                seps.append( item )
                leaves.append( [] )
            leaves[-1].append( item )

        if len( leaves[-1] ) < minimum and seps:
            last_two = leaves[-2] + leaves[-1]
            leaves[-2] = last_two[:minimum]
            leaves[-1] = last_two[minimum:]
            seps.append( last_two[minimum] )

        leaves = [self.LEAF( self,
                             contents=[p[0] for p in pairs],
                             data=[p[1] for p in pairs] )
                  for pairs in leaves]

        for i in range( len( leaves ) - 1 ):
            leaves[i].next = leaves[i + 1]

        return leaves, [s[0] for s in seps]


def test():
    b = BPlusTree( 3 )
    b.insert( 'a', 1 )
    b.insert( 'b', 2 )
    b.insert( 'c', 3 )
    b.insert( 'm', -1 )
    print( b.items() )
    print( b.keys() )
    print( b.values() )
    b.remove( 'a' )
    b.remove( 'b' )
    b.insert( 'dddddd', 1111111111 )
    print( b.items() )
    print( b.keys() )
    print( b.values() )


test()
