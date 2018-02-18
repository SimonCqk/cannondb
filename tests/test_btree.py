from cannondb.btree import BTree
from cannondb.constants import TreeConf

tree_conf = TreeConf(100, 8192, 16, 32)
test_file_name = 'tmp_tree'
tree = BTree(test_file_name)


def test_insert():
    tree.insert('a', 1)
    tree.insert('b', 2)
    tree.insert('c', 3)
    tree.remove('a')


if __name__ == '__main__':
    test_insert()
    print(tree['a'])

