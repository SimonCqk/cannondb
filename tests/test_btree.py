import time

from cannondb.btree import BTree

test_file_name = 'tmp_tree'
tree = BTree(test_file_name)


def test_normal_insert():
    tree.insert('a', 1, override=True)
    tree.insert('b', 2, override=True)
    tree.insert('c', 3, override=True)
    tree.commit()
    assert tree['a'] == 1
    assert tree['c'] == 3


def test_scale_insert():
    for i in range(0, 10000):
        tree.insert(str(i), i, override=True)
    tree.commit()


def test_range_get():
    for i in range(0, 10000):
        assert tree[str(i)] == i


def test_iter_self():
    iter_items = [i for i in tree]


def test_insert_float():
    tree.insert('f1', 1.1613168453135168, override=True)
    assert tree['f1'] == 1.1613168453135168
    tree.insert('f2', 1546646.55845454548, override=True)
    assert tree['f2'] == 1546646.55845454548
    tree.commit()


def test_insert_dict():
    tree.insert('d1', {'a': 1, 'b': 2, 'c': 3}, override=True)
    assert tree['d1'] == {'a': 1, 'b': 2, 'c': 3}
    d2 = {'d': -1, 'f': 'asd', 'test': 'inside'}
    tree.insert('d2', d2, override=True)
    assert tree['d2'] == d2
    tree.commit()


if __name__ == '__main__':
    # test_normal_insert()
    '''
    profile.run('test_scale_insert()', 'result.txt')
    p = pstats.Stats("result.txt")
    p.sort_stats("time").print_stats()
    '''
    tree.set_auto_commit(False)
    start = time.time()
    test_scale_insert()
    test_range_get()
    end = time.time()
    print(end - start)
    # test_iter_self()
    # test_insert_float()
    # test_insert_dict()
    tree.commit()
    tree.close()
