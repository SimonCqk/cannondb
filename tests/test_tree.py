import random

from cannondb.tree import BPlusTree

bpt = BPlusTree(50)


def test():
    bpt.insert('m', -1)
    bpt.insert('l', 12)
    bpt.insert('k', 13)
    bpt.insert('asd', 111)
    bpt.insert('a', 1)
    bpt.insert('bpt', 2)
    bpt.insert('c', 3)

    print(bpt.items())
    print(bpt.keys())
    print(bpt.values())

    bpt.remove('a')
    bpt.remove('bpt')
    bpt.remove('c')
    bpt.remove('asd')

    print(bpt.items())
    print(bpt.keys())
    print(bpt.values())


def test_tree_insert():
    tree = BPlusTree(10)
    for i in range(50):
        tree.insert(str(random.randint(0, 0xFFFFFFFF)), i)
    assert len(tree.items()) == 50 and \
           len(tree.keys()) == 50 and \
           len(tree.values()) == 50


if __name__ == '__main__':
    test()
