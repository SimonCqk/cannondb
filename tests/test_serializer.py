from uuid import UUID
from cannondb.serializer import IntSerializer, FloatSerializer, DictSerializer, ListSerializer, StrSerializer, \
    UUIDSerializer


def test_int_serializer():
    s = IntSerializer.serialize(1)
    assert IntSerializer.deserialize(s) == 1
    s = IntSerializer.serialize(-1)
    assert IntSerializer.deserialize(s) == -1
    s = IntSerializer.serialize(9999999)
    assert IntSerializer.deserialize(s) == 9999999
    s = IntSerializer.serialize(-9999999)
    assert IntSerializer.deserialize(s) == -9999999


def test_float_serializer():
    """
    convert float -> str and str -> float has unavoidable precision loss
    """
    s = FloatSerializer.serialize(0.01)
    assert abs(FloatSerializer.deserialize(s) - 0.01) < 0.00001
    s = FloatSerializer.serialize(-0.01)
    assert abs(FloatSerializer.deserialize(s) + 0.01) < 0.00001
    s = FloatSerializer.serialize(1234.56789)
    assert abs(FloatSerializer.deserialize(s) - 1234.56789) < 0.0001


def test_str_serializer():
    s = StrSerializer.serialize('cannondb')
    assert StrSerializer.deserialize(s) == 'cannondb'
    s = StrSerializer.serialize('php is the best language in the world')
    assert StrSerializer.deserialize(s) == 'php is the best language in the world'


def test_dict_serializer():
    s = DictSerializer.serialize({'a': 1, 'b': 2, 'c': 3})
    assert DictSerializer.deserialize(s) == {'a': 1, 'b': 2, 'c': 3}
    s = DictSerializer.serialize({'a': [1, 2, 3], 'b': ['a', 'b', 'c']})
    assert DictSerializer.deserialize(s) == {'a': [1, 2, 3], 'b': ['a', 'b', 'c']}
    s = DictSerializer.serialize({'a': 0.1, 'b': 'test', 'c': [1, 2, 3], 'd': {'a': 1, 'b': 2, 'c': 3}})
    assert DictSerializer.deserialize(s) == {'a': 0.1, 'b': 'test', 'c': [1, 2, 3], 'd': {'a': 1, 'b': 2, 'c': 3}}


def test_list_serializer():
    s = ListSerializer.serialize([1, 2, 3, 4])
    assert ListSerializer.deserialize(s) == [1, 2, 3, 4]
    s = ListSerializer.serialize(['1', '2', '3', '4'])
    assert ListSerializer.deserialize(s) == ['1', '2', '3', '4']
    s = ListSerializer.serialize(['test', 1, 'now', 2])
    assert ListSerializer.deserialize(s) == ['test', 1, 'now', 2]
    s = ListSerializer.serialize((1, 2, 3, 4, 5))
    assert ListSerializer.deserialize(s) == [1, 2, 3, 4, 5]


def test_uuid_serializer():
    u = UUID('{12345678-1234-5678-1234-567812345678}')
    s = UUIDSerializer.serialize(u)
    assert u.int == UUIDSerializer.deserialize(s).int
    u = UUID('urn:uuid:12345678-1234-5678-1234-567812345678')
    s = UUIDSerializer.serialize(u)
    assert u.int == UUIDSerializer.deserialize(s).int
