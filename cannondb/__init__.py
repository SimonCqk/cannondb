from cannondb.database import CannonDB
from cannondb.storages import FileStorage, MemoryStorage

__all__ = ('CannonDB', 'FileStorage', 'MemoryStorage')


def connect(file_name: str = 'database', cache_size: int = 128, *, order=100, page_size=8192, key_size=16,
            value_size=64, file_cache=1024, **kwargs):
    return CannonDB(file_name=file_name, cache_size=cache_size, order=order, page_size=page_size, key_size=key_size,
                    value_size=value_size, file_cache=file_cache, **kwargs)
