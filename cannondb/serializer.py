import json
import struct
from abc import ABCMeta, abstractmethod
from typing import Union, Type

from cannondb.constants import INT_FORMAT, FLOAT_FORMAT


class NoSerializerError(Exception):
    pass


class Serializer(metaclass=ABCMeta):
    __slots__ = []

    @abstractmethod
    def serialize(self, obj: object) -> bytes:
        """Serialize a key to bytes."""
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> object:
        """Create a key object from bytes."""
        pass

    def __repr__(self):
        return '{}()'.format(self.__class__.__name__)


class IntSerializer(Serializer):
    __slots__ = []

    def serialize(self, obj: int) -> bytes:
        return struct.pack(INT_FORMAT, obj)

    def deserialize(self, data: bytes) -> int:
        return struct.unpack(INT_FORMAT, data)[0]


class FloatSerializer(Serializer):
    __slots__ = []

    def serialize(self, obj: object) -> bytes:
        return struct.pack(FLOAT_FORMAT, obj)

    def deserialize(self, data: bytes) -> float:
        return struct.unpack(FLOAT_FORMAT, data)[0]


class StrSerializer(Serializer):
    __slots__ = []

    def serialize(self, obj: str) -> bytes:
        return obj.encode(encoding='utf-8')

    def deserialize(self, data: bytes) -> str:
        return data.decode(encoding='utf-8')


class DictSerializer(Serializer):
    __slots__ = []

    def serialize(self, obj: object) -> bytes:
        return json.dumps(obj, ensure_ascii=False).encode(encoding='utf-8')

    def deserialize(self, data: bytes) -> dict:
        return json.loads(data.decode(encoding='utf-8'))


# make it a global var
serializer_map = {
    int: IntSerializer,
    float: FloatSerializer,
    str: StrSerializer,
    dict: DictSerializer
}

type_num_map = {
    0: int,
    1: float,
    2: str,
    3: dict,
    int: 0,
    float: 1,
    str: 2,
    dict: 3
}


def serializer_switcher(t: Type[Union[int, float, str, dict]]):
    """return corresponding serializer to arg type"""
    try:
        return serializer_map[t]
    except KeyError:
        raise NoSerializerError('No corresponding serializer')


def type_switcher(num: Union[Type, int]) -> Union[Type[Union[int, float, str, dict], int]]:
    """return type(type-num) by type-num(type)"""
    try:
        return type_num_map[num]
    except KeyError:
        raise ValueError('No corresponding type to number {}'.format(num))
