import time

from cannondb.btree import BTree
from cannondb.constants import TreeConf

tree_conf = TreeConf(100, 8192, 16, 32)
test_file_name = 'tmp_tree'
tree = BTree(test_file_name)

now = lambda: time.time()


def test_insert():
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)


def test_scale_insert():
    for i in range(10000):
        tree.insert(str(i), i, override=True)


def test_iter_self():
    iter_items = []
    for item in tree:
        print(item)
        iter_items.append(item)


if __name__ == '__main__':
    test_iter_self()
