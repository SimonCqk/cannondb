from cannondb.constants import METHODS_TO_LOG
from cannondb.database import CannonDB
from cannondb.storages import FileStorage, MemoryStorage
from cannondb.wrapper import log_wrapper

__version__ = '1.1.0'

__all__ = ('CannonDB', 'FileStorage', 'MemoryStorage')


def connect(file_name: str = 'database', cache_size: int = 128, *, order=100, page_size=8192, key_size=16,
            value_size=64, file_cache=1024, **kwargs):
    db = CannonDB(file_name=file_name, cache_size=cache_size, order=order, page_size=page_size, key_size=key_size,
                  value_size=value_size, file_cache=file_cache, **kwargs)
    log_config = kwargs.pop('log', None)

    if log_config == 'tcp' or log_config == 'udp':
        host, port = kwargs.pop('host', None), kwargs.pop('port', None)
        if host is None or port is None:
            raise ValueError('Host and port of Log Socket should be specified')
        db = log_wrapper(db, METHODS_TO_LOG, config=log_config, host=host, port=port)
    elif log_config == 'local':
        db = log_wrapper(db, METHODS_TO_LOG, config=log_config)

    return db
