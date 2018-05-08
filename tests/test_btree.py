import random

from cannondb.btree import BTree

TEST_RANDOM_NUMS = 100000
test_file_name = 'tmp/tmp_tree'


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


def test_scale_insert():
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


if __name__ == '__main__':
    test_scale_insert()
