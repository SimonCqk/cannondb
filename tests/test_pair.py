from cannondb.constants import TreeConf
from cannondb.node import KeyValPair
from cannondb.utils import write_to_file, read_from_file, open_database_file

tree_conf = TreeConf(100, 8192, 16, 32)

test_file_name = 'test_tmp'


def test_normal():
    f = open_database_file(test_file_name)
    orig = KeyValPair(tree_conf, 'test', 1)
    as_bytes = orig.dump()
    assert len(as_bytes) == orig.length
    write_to_file(f, as_bytes)
    # print(as_bytes)
    data = read_from_file(f, 0, len(as_bytes))
    after = KeyValPair(tree_conf, data=data)
    print('_key=', after._key)
    print('_value=', after._value)
    del after
    after = KeyValPair(tree_conf)
    after.load(data)
    print('_key=', after._key)
    print('_value=', after._value)


def test_dict_value():
    orig = KeyValPair(tree_conf, 'test', {'1': 1, '2': 2, '3': 3})
    as_bytes = orig.dump()
    assert len(as_bytes) == orig.length
    # print(as_bytes)
    after = KeyValPair(tree_conf, data=as_bytes)
    print('_key=', after._key)
    print('_value=', after._value)
    del after
    after = KeyValPair(tree_conf)
    after.load(as_bytes)
    print('_key=', after._key)
    print('_value=', after._value)


def test_float_value():
    orig = KeyValPair(tree_conf, 'test', 1.154316461348465464)
    as_bytes = orig.dump()
    assert len(as_bytes) == orig.length
    # print(as_bytes)
    after = KeyValPair(tree_conf, data=as_bytes)
    print('_key=', after._key)
    print('_value=', after._value)
    del after
    after = KeyValPair(tree_conf)
    after.load(as_bytes)
    print('_key=', after._key)
    print('_value=', after._value)


if __name__ == '__main__':
    test_normal()
    test_dict_value()
    test_float_value()
