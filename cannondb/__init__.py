from cannondb.utils import open_database_file
from .database import CannonDB

__all__ = []


def connect(db_name):
    return CannonDB(file_name=db_name)
