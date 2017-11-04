'''
This file include:
- Storage: Abstract base class for all storage implementation.
- FileStorage: Store data into disk.
- MemoryStorage: Store data into memory.
'''

import multiprocessing
import os
import struct
from abc import ABCMeta, abstractmethod

import portalocker

from .utility import with_metaclass, generate_address, OutOfAddressException


class Storage(with_metaclass(ABCMeta, object)):
    '''
    Abstract base class for all storage implementation.
    Subclasses must override all these methods.
    '''

    @abstractmethod
    def read(self, address):
        raise NotImplementedError("Please override this method.")

    @abstractmethod
    def write(self, data, address):
        raise NotImplementedError("Please override this method.")

    @abstractmethod
    def close(self):
        raise NotImplementedError("Please override this method.")

    # lock() & unlock ensure the data safety under multi-readers/writers
    # scenarios.
    def lock(self):
        pass

    def unlock(self):
        pass


class FileStorage(Storage):
    '''
    Store data into disk (.db file).
    '''
    ROOT_BLOCK_SIZE = 4096
    # byte-order:network (= big-endian) type:unsigned long long
    INTEGER_FORMAT = "!Q"
    INTEGER_LENGTH = 8  # sizeof(unsigned long long) = 8

    def __init__(self, file):
        super(FileStorage, self).__init__()
        self._file = file
        self.locked = False
        self._ensure_root_block()

    def _ensure_root_block(self):
        self.lock()
        self._seek_end()
        end_address = self._file.tell()
        if end_address < self.ROOT_BLOCK_SIZE:
            self._file.write(b'\x00' * (self.ROOT_BLOCK_SIZE - end_address))
        self.unlock()

    def _seek_end(self):
        self._file.seek(0, os.SEEK_END)  # move to the end of file

    def _seek_root_block(self):
        self._file.seek(0)  # move to the start of file

    def _bytes_to_integer(self, integer_bytes):
        return struct.unpack(self.INTEGER_FORMAT, integer_bytes)[0]

    def _integer_to_bytes(self, integer):
        return struct.pack(self.INTEGER_FORMAT, integer)

    def _read_integer(self):
        return self._bytes_to_integer(self._file.read(self.INTEGER_LENGTH))

    def _write_integer(self, integer):
        self.lock()
        self._file.write(self._integer_to_bytes(integer))

    def _deprecate_old(self, address):
        self._file.seek(address)
        size = self._read_integer()
        size += len(size)
        # overwrite [block-size][block] by empty str.
        self._file.write(b' ' * size)

    def commit_root_address(self, root_address):
        self.lock()
        self._file.flush()
        self._seek_root_block()
        self._write_integer(root_address)
        self._file.flush()
        self.unlock()

    def get_root_address(self):
        self._seek_root_block()
        root_address = self._read_integer()
        return root_address

    def write(self, data, address):
        self.lock()
        if not address:  # write for the first time, just append.
            self._seek_end()
            obj_address = self._file.tell()
        else:
            self._file.seek(address)
            # can be over-writen in the origin location, for saving memory.
            if len(data) <= self._read_integer():
                self._file.seek(address)
                obj_address = address
            else:
                self._deprecate_old(address)
                self._seek_end()
                obj_address = self._file.tell()
        # 1.write the length of data  2.write the real data
        self._write_integer(len(data))
        self._file.write(data)
        self.unlock()
        return obj_address

    def read(self, address):
        if address - \
                self._file.seek(0) > self._seek_end() - self._file.seek(0):
            raise OutOfAddressException('Out of address in this file.')
        self._file.seek(address)
        length = self._read_integer()
        data = self._file.read(length)
        return data

    def close(self):
        self.unlock()
        self._file.close()

    def lock(self):
        if not self.locked:
            portalocker.lock(self._file, portalocker.LOCK_EX)
            self.locked = True

    def unlock(self):
        if self.locked:
            self._file.flush()
            portalocker.unlock(self._file)
            self.locked = False

    @property
    def closed(self):
        return self._file.closed


class MemoryStorage(Storage):
    '''
    Store data just in memory.
    '''
    __slots__ = ['memory', 'lock']  # to save memory.

    def __init__(self):
        super(MemoryStorage, self).__init__()
        self.memory = dict()
        self.lock = multiprocessing.Lock()

    def write(self, data, address):
        with self.lock:
            if address == 0:
                address = generate_address(self.memory)
            self.memory[address] = data
        return address

    def read(self, address):
        return self.memory[address]

    def close(self):
        self.memory.clear()
