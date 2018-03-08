"""
  This file include storage wrappers to expand the functionality of storage.
- log_wrapper: Use logs to record all pivotal operations.
"""
import datetime
import functools
import logging
import os
from logging import handlers as log_handlers

_log_file_name = 'log.log'

# if in debug mode
if __debug__:

    def _log_wrapper(func, logger: logging.Logger):
        logger.setLevel(logging.DEBUG)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log = 'Called: ' + func.__name__ + '('
            # !r means call __repr__ only / !s means call __str__ only
            # for safety, DO NOT record parameters of 'insert' operation.
            log += ','.join(['{0!r}'.format(a) for a in args[1:]] + ['{0!s}={1!r}'.format(k, v) for k, v in
                                                                     kwargs.items()])
            exception = None
            try:
                return func(*args, **kwargs)
            except Exception as error:
                exception = error
            finally:
                log += ')' if exception is None else ") {0}: {1}".format(type(exception), exception)
                log += 'at {time}'.format(time=datetime.datetime.now().isoformat())
                logger.debug(log)
                if exception is not None:
                    raise exception

        return wrapper

else:
    def _log_wrapper(func, logger: logging.Logger):
        logger.setLevel(logging.INFO)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log = 'Called:' + func.__name__ + '('
            # for safety, DO NOT record parameters of 'insert' operation.
            log += ','.join(['{0}'.format(a) for a in args[1:]] + ['{0}={1}'.format(k, v) for k, v in
                                                                   kwargs.items()])
            log += ') at {time}'.format(time=datetime.datetime.now().isoformat())
            logger.info(log)
            return func(*args, **kwargs)

        return wrapper


def log_wrapper(cls, methods_to_log: tuple, config='local', host=None, port=None):
    orig_cls = cls.__class__
    logger = logging.getLogger(orig_cls.__name__)
    if config == 'tcp' or config == 'udp':
        handler = log_handlers.SocketHandler(host=host, port=port) if config == 'tcp' else log_handlers.DatagramHandler(
            host=host, port=port)
    else:
        if not os.path.exists(_log_file_name):
            os.open(_log_file_name, os.O_RDWR | os.O_CREAT)
        handler = logging.FileHandler(_log_file_name, mode='r+')
    logger.addHandler(handler)

    orig_methods = [getattr(orig_cls, method) for method in methods_to_log if hasattr(orig_cls, method)]

    logged_methods = [_log_wrapper(method, logger) for method in orig_methods]

    for method in logged_methods:
        setattr(orig_cls, method.__name__, method)

    # bind logger with instance, so as to close log-handler when db closes
    cls._logger = logger
    return cls
