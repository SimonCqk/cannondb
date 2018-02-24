from cannondb.btree import BTree

file_name = 'test_overflow'
tree = BTree(file_name, 3, 32, 8, 12, cache_size=0)

"""
tree config set as: order=3, page size=32, key size=8, value size=12
root node raw overflow_data should be:b'\x00\x00T\x00\x00\x00\x00\x00\x02\x00\x041234
\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x04\xd2\x00\x00\x00\x00\x00\x00
\x00\x00\x00\x00\x044567\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x11\xd7
\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x046789\x00\x00\x00\x00\x02\x00\x00
\x00\x04\x00\x00\x1a\x85\x00\x00\x00\x00\x00\x00\x00\x00\x00'
, after combining all overflow overflow_data, it should be matched.
"""


def test_overflow():
    tree.insert('1234', 1234, override=True)
    tree.insert('4567', 4567, override=True)
    tree.insert('6789', 6789, override=True)


if __name__ == '__main__':
    test_overflow()
