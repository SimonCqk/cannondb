import random
from tests.util import refine_test_file

from cannondb.btree import BTree
from cannondb.constants import TreeConf

TEST_RANDOM_NUMS = 100000
test_file_name = refine_test_file('test_tree')


def test_normal_insert():
    tree = BTree(test_file_name)
    tree.insert('a', 1, override=True)
    tree.insert('b', 2, override=True)
    tree.insert('123', 'python', override=True)
    tree.insert('list', [2, 3, 4], override=True)
    tree.insert('dict', {1: 1, 2: 2, 3: 3}, override=True)
    tree.commit()
    assert tree['a'] == 1
    assert tree['b'] == 2
    assert tree.get('123') == 'python'
    assert tree.get('list') == [2, 3, 4]
    assert tree.get('dict') == {1: 1, 2: 2, 3: 3}
    tree.close()


def __test_scale_insert():
    """ Time-consumed """
    tree = BTree(test_file_name)
    nums = [random.randrange(0, 0xFFFFFF) for _ in range(TEST_RANDOM_NUMS)]
    tree.set_auto_commit(False)
    for num in nums:
        tree.insert(str(num), num, override=True)
    tree.commit()
    tree.close()
    tree = BTree(test_file_name)
    for num in nums:
        assert tree.get(str(num)) == num
    tree.commit()
    tree.close()


def test_insert_float():
    tree = BTree(test_file_name)
    tree.insert('f1', 1.1613168453135168, override=True)
    assert tree['f1'] == 1.1613168453135168
    tree.insert('f2', 1546646.55845454548, override=True)
    assert tree['f2'] == 1546646.55845454548
    tree.commit()
    tree.close()


def test_insert_dict():
    tree = BTree(test_file_name)
    tree.insert('d1', {'a': 1, 'b': 2, 'c': 3}, override=True)
    assert tree['d1'] == {'a': 1, 'b': 2, 'c': 3}
    d2 = {'d': -1, 'f': 'asd', 'test': 'inside'}
    tree.insert('d2', d2, override=True)
    assert tree['d2'] == d2
    tree.commit()
    tree.close()


def test_insert_list():
    tree = BTree(test_file_name)
    tree.insert('l1', [1, 2, 3], override=True)
    tree.insert('l2', [2.1, 'cannon', 0], override=True)
    assert tree['l1'] == [1, 2, 3]
    assert tree['l2'] == [2.1, 'cannon', 0]
    tree.commit()
    tree.close()


"""
test_tree config set as: order=3, page size=32, key size=8, value size=12
root node raw overflow_data should be:b'\x00\x00T\x00\x00\x00\x00\x00\x02\x00\x041234
\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x04\xd2\x00\x00\x00\x00\x00\x00
\x00\x00\x00\x00\x044567\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x11\xd7
\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x046789\x00\x00\x00\x00\x02\x00\x00
\x00\x04\x00\x00\x1a\x85\x00\x00\x00\x00\x00\x00\x00\x00\x00'
, after combining all overflow overflow_data, it should be matched.
"""


def test_overflow():
    test_of_name = refine_test_file('test_overflow')
    test_tree = BTree(test_of_name, 3, 32, 8, 12, 0)
    test_tree.insert('1234', 1234, override=True)
    test_tree.insert('4567', 4567, override=True)
    test_tree.insert('6789', 6789, override=True)
    assert test_tree['1234'] == 1234
    assert test_tree['4567'] == 4567
    assert test_tree['6789'] == 6789


if __name__ == '__main__':
    test_overflow()
