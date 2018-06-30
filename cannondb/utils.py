"""
  This include some helper-functions or classes.
"""
import io
import math
import os


class EndOfFileError(Exception):
    pass


def open_database_file(file_name, suffix='.cdb'):
    """
    Open a file in binary mode, if not exist then create it
    """
    if os.path.exists(file_name):
        f = open(file_name + suffix, 'rb+', buffering=0)
    else:
        fd = os.open(file_name + suffix, os.O_RDWR | os.O_CREAT)
        f = os.fdopen(fd, 'rb+')
    return f


def file_flush_and_sync(f: io.FileIO):
    """
    Call system sync, ensure write the file’s overflow_data to disk, but it's a EXPENSIVE op
    """
    f.flush()
    os.fsync(f.fileno())


def read_from_file(file_fd: io.FileIO, start: int, stop: int) -> bytes:
    length = stop - start
    assert length >= 0
    file_fd.seek(start)
    data = bytes()
    while file_fd.tell() < stop:
        read_data = file_fd.read(stop - file_fd.tell())
        if read_data == b'':
            raise EndOfFileError('Read until the end of file_fd')
        data += read_data
    assert len(data) == length
    return data


def write_to_file(file_fd: io.FileIO, data: bytes, f_sync: bool = False):
    length_to_write = len(data)
    written = 0
    while written < length_to_write:
        written = file_fd.write(data[written:])
    if f_sync:
        file_flush_and_sync(file_fd)


def generate_address(data):
    """
    Generate a address dynamically by hashing.
    """
    if not isinstance(data, str):
        data = str(data)
    return hash(data) ^ (hash(data) >> 2)


def is_power_of_2(num: int) -> bool:
    """
    If a number is power of 2, then the binary looks like: 0x...100000...0,
    and (number - 1) looks like:                           0x...011111...1.
    so do bit operation &, we got 0.
    """
    return (num & (num - 1)) is 0


def adjust_to_power_of_2(num: int) -> int:
    """
    In case of performance, we'd better define the some sizes of data structures or range limitations...
    etc, to be the power of 2.
    :param num: number passed in.
    :return: if number is power of 2, just return it, else return the smallest 2 powers which
             just greater than `num`.
    """
    if is_power_of_2(num):
        return num
    else:
        log2 = int(math.log2(num))
        return pow(2, log2 + 1)


class FakeCache(object):
    """A cache that doesn't cache anything."""

    def get(self, k, d=None):
        pass

    def __setitem__(self, key, value):
        pass

    def clear(self):
        pass


class LRUCache(dict):

    def __init__(self, *args, **kwargs):
        """
        :param capacity: How many items to store before cleaning up old items
                         or ``None`` for an unlimited cache size
        """

        self.capacity = kwargs.pop('capacity', None) or float('nan')
        self.lru = []

        super(LRUCache, self).__init__(*args, **kwargs)

    def refresh(self, key):
        """
        Push a key to the tail of the LRU queue
        """
        if key in self.lru:
            self.lru.remove(key)
        self.lru.append(key)

    def get(self, key, default=None):
        item = super(LRUCache, self).get(key, default)
        self.refresh(key)

        return item

    def __getitem__(self, key):
        item = super(LRUCache, self).__getitem__(key)
        self.refresh(key)

        return item

    def __setitem__(self, key, value):
        super(LRUCache, self).__setitem__(key, value)

        self.refresh(key)

        # Check, if the cache is full and we have to remove old items
        if len(self) > self.capacity:
            self.pop(self.lru.pop(0))

    def __delitem__(self, key):
        super(LRUCache, self).__delitem__(key)
        self.lru.remove(key)

    def clear(self):
        super(LRUCache, self).clear()
        del self.lru[:]


def with_metaclass(meta, *bases):
    """
    Function from jinja2/_compat.py. License: BSD.
    Use it like this::

        class BaseForm(object):
            pass

        class FormType(type):
            pass

        class Form(with_metaclass(FormType, BaseForm)):
            pass
    This requires a bit of explanation: the basic idea is to make a
    dummy metaclass for one level of class instantiation that replaces
    itself with the actual metaclass.  Because of internal type checks
    we also need to make sure that we downgrade the custom metaclass
    for one level to something closer to type (that's why __call__ and
    __init__ comes back from type etc.).

    This has the advantage over six.with_metaclass of not introducing
    dummy classes into the final MRO.
    """

    class MetaClass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)

    return MetaClass('temporary_class', None, {})
