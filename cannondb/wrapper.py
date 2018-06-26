"""
Log wrapper record pivotal operations along with some information into log file. It's just a wrapper
used like function-wrapper, add small feature although they look flashy...
"""
import datetime
import functools
import logging
import os
from logging import handlers as log_handlers
from platform import system

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


def log_wrapper(cls, methods_to_log: tuple, log_mode='local', host=None, port=None):
    """
    :param cls: instance to be logged
    :param methods_to_log: methods of instance to be logged (both method name, parameters, time
                           of invoking will be logged)
    :param log_mode: 'local': log in local file (log.log)
                     'tcp' or 'udp': log to concrete host & port
    :param host: target host if log mode is 'tcp' or 'udp'
    :param port: port of target host if log mode is 'tcp' or 'udp'
    :return: wrapped instance
    """
    orig_cls = cls.__class__
    logger = logging.getLogger(orig_cls.__name__)
    if log_mode == 'tcp' or log_mode == 'udp':
        handler = log_handlers.SocketHandler(host=host,
                                             port=port) if log_mode == 'tcp' else log_handlers.DatagramHandler(
            host=host, port=port)
    else:
        if not os.path.exists(_log_file_name):
            if system() == 'Windows':
                open(_log_file_name, 'a').close()
            else:
                os.mknod(_log_file_name)
        handler = logging.FileHandler(_log_file_name, mode='r+')
    logger.addHandler(handler)

    orig_methods = [getattr(orig_cls, method) for method in methods_to_log if hasattr(orig_cls, method)]

    logged_methods = [_log_wrapper(method, logger) for method in orig_methods]

    for method in logged_methods:
        setattr(orig_cls, method.__name__, method)

    # bind logger with instance, so as to close log-handler when db closes
    cls._logger = logger
    return cls
