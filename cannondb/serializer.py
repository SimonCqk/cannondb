import struct
from abc import ABCMeta, abstractmethod
from typing import Union, Type

from cannondb.const import INT_FORMAT, FLOAT_FORMAT


class SerializerError(Exception):
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


# make it a global var
type_map = {
    int: IntSerializer,
    float: FloatSerializer,
    str: StrSerializer
}


def serializer_type_switch(t: Type[Union[int, float, str]]):
    """return corresponding serializer to arg type"""
    try:
        return type_map[t]
    except TypeError:
        raise SerializerError('No corresponding serializer')
