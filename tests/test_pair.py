from cannondb.constants import TreeConf
from cannondb.node import KeyValPair

tree_conf = TreeConf(100, 8192, 16, 32)


def test_str():
    test_key, test_val = 'test', 'hello cannon'
    orig = KeyValPair(tree_conf, test_key, test_val)
    as_bytes = orig.dump()
    assert len(as_bytes) == orig.length
    after = KeyValPair(tree_conf, data=as_bytes)
    assert after.key == test_key and after.value == test_val


def test_int_and_float():
    """bytes convert to float may generate accuracy-error"""
    test_key_i, test_val_i = 111, 222
    test_key_f, test_val_f = 1.12348, 3.01231
    orig_i = KeyValPair(tree_conf, test_key_i, test_val_i)
    orig_f = KeyValPair(tree_conf, test_key_f, test_val_f)
    as_bytes_i = orig_i.dump()
    as_bytes_f = orig_f.dump()
    assert len(as_bytes_i) == orig_i.length and len(as_bytes_f) == orig_f.length
    after_i = KeyValPair(tree_conf, data=as_bytes_i)
    after_f = KeyValPair(tree_conf, data=as_bytes_f)
    assert abs(after_i.key - test_key_i) < 0.00001 and abs(after_i.value - test_val_i) < 0.00001
    assert abs(after_f.key - test_key_f) < 0.00001 and abs(after_f.value - test_val_f) < 0.00001


def test_list():
    test_key1, test_value1 = 'test', [1, 2, 3, 4]
    test_key2, test_value2 = 'hello', [1.123, 4, 'python']
    orig1 = KeyValPair(tree_conf, test_key1, test_value1)
    orig2 = KeyValPair(tree_conf, test_key2, test_value2)
    as_bytes1 = orig1.dump()
    as_bytes2 = orig2.dump()
    assert len(as_bytes1) == orig1.length and len(as_bytes2) == orig2.length
    after1 = KeyValPair(tree_conf, data=as_bytes1)
    after2 = KeyValPair(tree_conf, data=as_bytes2)
    assert after1.key == test_key1 and after1.value == test_value1
    assert after2.key == test_key2 and after2.value == test_value2


def test_dict():
    test_key, test_val = 'test', {'1': 1, '2': 2, '3': 3}
    orig = KeyValPair(tree_conf, test_key, test_val)
    as_bytes = orig.dump()
    assert len(as_bytes) == orig.length
    after = KeyValPair(tree_conf, data=as_bytes)
    assert after.key == test_key and after.value == test_val
