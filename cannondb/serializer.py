import json
import struct
from abc import ABCMeta
from uuid import UUID

from cannondb.constants import INT_FORMAT, FLOAT_FORMAT


class NoSerializerError(Exception):
    pass


class Serializer(metaclass=ABCMeta):
    __slots__ = []

    @staticmethod
    def serialize(obj: object) -> bytes:
        """Serialize a _key to bytes."""
        raise NotImplementedError

    @staticmethod
    def deserialize(data: bytes) -> object:
        """Create a _key object from bytes."""
        raise NotImplementedError

    def __repr__(self):
        return '{}()'.format(self.__class__.__name__)


class IntSerializer(Serializer):
    __slots__ = []

    @staticmethod
    def serialize(obj: int) -> bytes:
        return struct.pack(INT_FORMAT, obj)

    @staticmethod
    def deserialize(data: bytes) -> int:
        return struct.unpack(INT_FORMAT, data)[0]


class FloatSerializer(Serializer):
    __slots__ = []

    @staticmethod
    def serialize(obj: object) -> bytes:
        return struct.pack(FLOAT_FORMAT, obj)

    @staticmethod
    def deserialize(data: bytes) -> float:
        return struct.unpack(FLOAT_FORMAT, data)[0]


class StrSerializer(Serializer):
    __slots__ = []

    @staticmethod
    def serialize(obj: str) -> bytes:
        return obj.encode(encoding='utf-8')

    @staticmethod
    def deserialize(data: bytes) -> str:
        return data.decode(encoding='utf-8')


class DictSerializer(Serializer):
    __slots__ = []

    @staticmethod
    def serialize(obj: dict) -> bytes:
        return json.dumps(obj, ensure_ascii=False).encode(encoding='utf-8')

    @staticmethod
    def deserialize(data: bytes) -> dict:
        return json.loads(data.decode(encoding='utf-8'))


# both list and tuple are supported, but elements can only be json types.
class ListSerializer(Serializer):
    __slots__ = []

    @staticmethod
    def serialize(obj: [list, tuple]) -> bytes:
        return json.dumps(obj, ensure_ascii=False).encode(encoding='utf-8')

    @staticmethod
    def deserialize(data: bytes) -> [list, tuple]:
        return json.loads(data.decode(encoding='utf-8'))


class UUIDSerializer(Serializer):
    __slots__ = []

    @staticmethod
    def serialize(obj: UUID) -> bytes:
        return obj.bytes

    @staticmethod
    def deserialize(data: bytes) -> UUID:
        return UUID(bytes=data)


# make it a global var
serializer_map = {
    int: IntSerializer(),
    float: FloatSerializer(),
    str: StrSerializer(),
    dict: DictSerializer(),
    list: ListSerializer(),
    UUID: UUIDSerializer()
}

type_num_map = {
    0: int,
    1: float,
    2: str,
    3: dict,
    4: list,
    5: UUID
}

type_num_map.update(dict(zip(type_num_map.values(), type_num_map.keys())))


def serializer_switcher(t: [int, float, str, dict, list, UUID]) -> Serializer:
    """return corresponding serializer to arg type"""
    try:
        return serializer_map[t]
    except KeyError:
        raise NoSerializerError('No corresponding serializer')


def type_switcher(num_or_type):
    """return type(type-num) by type-num(type)"""
    try:
        return type_num_map[num_or_type]
    except KeyError:
        raise ValueError('No corresponding type to number {}'.format(num_or_type))
