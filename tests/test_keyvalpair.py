from cannondb.constants import TreeConf
from cannondb.node import KeyValPair

tree_conf = TreeConf(100, 8192, 16, 32)


def test_normal():
    orig = KeyValPair(tree_conf, 'test', 1)
    as_bytes = orig.dump()
    assert len(as_bytes) == orig.length
    # print(as_bytes)
    after = KeyValPair(tree_conf, data=as_bytes)
    print('key=', after.key)
    print('value=', after.value)
    del after
    after = KeyValPair(tree_conf)
    after.load(as_bytes)
    print('key=', after.key)
    print('value=', after.value)


def test_dict_value():
    orig = KeyValPair(tree_conf, 'test', {'1': 1, '2': 2, '3': 3})
    as_bytes = orig.dump()
    assert len(as_bytes) == orig.length
    # print(as_bytes)
    after = KeyValPair(tree_conf, data=as_bytes)
    print('key=', after.key)
    print('value=', after.value)
    del after
    after = KeyValPair(tree_conf)
    after.load(as_bytes)
    print('key=', after.key)
    print('value=', after.value)


def test_float_value():
    orig = KeyValPair(tree_conf, 'test', 1.154316461348465464)
    as_bytes = orig.dump()
    assert len(as_bytes) == orig.length
    # print(as_bytes)
    after = KeyValPair(tree_conf, data=as_bytes)
    print('key=', after.key)
    print('value=', after.value)
    del after
    after = KeyValPair(tree_conf)
    after.load(as_bytes)
    print('key=', after.key)
    print('value=', after.value)


if __name__ == '__main__':
    test_normal()
    test_dict_value()
    test_float_value()
