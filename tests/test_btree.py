import random

from cannondb.btree import BTree

tree = BTree()


def test_insert():
    items = []
    cur = random.randint(0, 0xFFFF)
    while len(items) < 10:
        items.append(cur)
        cur = random.randint(0, 0xFFFF)
        while cur in items:
            cur = random.randint(0, 0xFFFF)
    for i in items:
        tree.insert(str(i), i)
    assert tree.count == 10
    for i in tree:
        print(i)
    for i in tree:
        tree.remove(i[0])


def test_remove():
    items = [i for i in tree]
    for i in items:
        tree.remove(i[0])
    assert tree.count == 0


def test_exist_insert():
    tree.insert('1', 1)
    tree.insert('2', 2)
    tree.insert('3', 3)
    assert tree.count == 3
    try:
        tree.insert('1', 1)
    except ValueError as e:
        print(e)
    tree.remove('1')
    tree.remove('2')
    tree.remove('3')


def test_non_exist_remove():
    try:
        tree.remove('-1')
    except ValueError as e:
        print(e)


def test_tree_massive_insert():
    for i in range(10000):
        tree.insert(str(i), i)
    items = [i for i in tree]
    assert tree.count == 10000 and len(items) == 10000


def test_tree_massive_remove():
    for i in range(10000):
        tree.remove(str(i))
    items = [i for i in tree]
    assert tree.count == 0 and len(items) == 0


if __name__ == '__main__':
    test_insert()
    test_remove()
    test_exist_insert()
    test_non_exist_remove()
    test_tree_massive_insert()
    test_tree_massive_remove()
