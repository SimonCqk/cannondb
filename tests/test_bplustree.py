from cannondb.bplustree import BPlusTree

bpt = BPlusTree(3)


def test():
    bpt.insert('a', 1)
    bpt.insert('bpt', 2)
    bpt.insert('c', 3)
    bpt.insert('m', -1)
    print(bpt.items())
    print(bpt.keys())
    print(bpt.values())
    bpt.remove('bpt')
    bpt.remove('a')
    bpt.remove('m')
    print(bpt.items())
    print(bpt.keys())
    print(bpt.values())


if __name__=='__main__':
    test()