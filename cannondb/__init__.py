import os
from .database import CannonDB

__all__ = []


def _create_file(fname, suffix='.txt', mode='r+'):
	try:
		f = open(fname + suffix, mode)
	except IOError:
		fd = os.open(fname, os.O_RDWR | os.O_CREAT)
		f = os.fdopen(fd + suffix, mode)
	return f


def connect(dbname):
	f = _create_file(dbname, suffix='.db', mode='rb+')
	return CannonDB(file=f)
