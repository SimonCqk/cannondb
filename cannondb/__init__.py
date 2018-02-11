from cannondb.utils import open_database_file
from .database import CannonDB

__all__ = []


def connect(db_name):
    f = open_database_file(db_name)
    return CannonDB(file=f)
