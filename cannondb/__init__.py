import os

from .database import CannonDB

__all__ = []


def _create_database(file_name, suffix='.txt', mode='r+'):
    try:
        f = open(file_name + suffix, mode)
    except IOError:
        fd = os.open(file_name + suffix, os.O_RDWR | os.O_CREAT)
        f = os.fdopen(fd, mode)
    return f


def connect(db_name):
    f = _create_database(db_name, suffix='.db', mode='ab+')
    return CannonDB(file=f)
