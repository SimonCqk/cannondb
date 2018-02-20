from cannondb.btree import BTree

test_file_name = 'tmp_tree'
tree = BTree(test_file_name)


def test_normal_insert():
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    assert tree['a'] == 1
    assert tree['c'] == 3


def test_scale_insert():
    for i in range(0, 10000):
        tree.insert(str(i), i, override=True)
    assert tree['1'] == 1
    assert tree['100'] == 100
    assert tree['5000'] == 5000
    assert tree['9999'] == 9999


def test_iter_self():
    iter_items = []
    for item in tree:
        iter_items.append(item)
    assert len(iter_items) == 10003


def test_insert_float():
    tree.insert('f1', 1.1613168453135168, override=True)
    assert tree['f1'] == 1.1613168453135168
    tree.insert('f2', 1546646.55845454548, override=True)
    assert tree['f2'] == 1546646.55845454548


def test_insert_dict():
    tree.insert('d1', {'a': 1, 'b': 2, 'c': 3}, override=True)
    assert tree['d1'] == {'a': 1, 'b': 2, 'c': 3}
    d2 = {'d': -1, 'f': 'asd', 'test': 'inside'}
    tree.insert('d2', d2, override=True)
    assert tree['d2'] == d2


if __name__ == '__main__':
    # test_normal_insert()
    # test_scale_insert()
    # test_iter_self()
    test_insert_float()
    test_insert_dict()
