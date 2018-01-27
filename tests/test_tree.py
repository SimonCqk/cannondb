from cannondb.tree import BPlusTree

bpt = BPlusTree(3)


def test():
    bpt.insert('a', 1)
    bpt.insert('bpt', 2)
    bpt.insert('c', 3)
    bpt.insert('m', -1)
    bpt.insert('l', 12)
    bpt.insert('k', 13)
    bpt.insert('asd', 111)
    print(bpt.items())
    print(bpt.keys())
    print(bpt.values())


'''
    bpt.remove('bpt')
    bpt.remove('a')
    bpt.remove('m')
    bpt.remove('c')
    print(bpt.items())
    print(bpt.keys())
    print(bpt.values())
'''

if __name__ == '__main__':
    test()
